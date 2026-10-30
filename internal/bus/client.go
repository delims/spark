package bus

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"
)

// Client is an agent's handle to the bus.
type Client struct {
	Layout *Layout
	Agent  string // display name of this agent
}

// NewClient builds a client for the given agent name and SPARK_BASE.
func NewClient(agent, base string) (*Client, error) {
	l, err := NewLayout(base)
	if err != nil {
		return nil, err
	}
	if err := l.EnsureDirs(); err != nil {
		return nil, err
	}
	if agent == "" {
		agent = "anon"
	}
	return &Client{Layout: l, Agent: agent}, nil
}

// EnsureWriter makes sure a writer daemon is running, autostarting one in the
// background if needed. The flock inside the daemon guarantees only one wins
// even if several agents race to start it.
func (c *Client) EnsureWriter() error {
	if WriterRunning(c.Layout) {
		return nil
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	cmd := exec.Command(exe, "writer")
	cmd.Env = append(os.Environ(), "SPARK_BASE="+c.Layout.Base)
	// Fully detach: new session, no controlling terminal, no inherited stdio.
	cmd.Stdin, cmd.Stdout, cmd.Stderr = nil, nil, nil
	cmd.SysProcAttr = detachAttr()
	if err := cmd.Start(); err != nil {
		return err
	}
	_ = cmd.Process.Release()
	// Give it a moment to grab the lock and write its PID.
	for i := 0; i < 20; i++ {
		if WriterRunning(c.Layout) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil // not fatal; Send falls back to direct write
}

// Send submits a message. It appends to the write queue and ensures the writer
// daemon is up. If the daemon is not running, it safely drains the queue itself
// under the exclusive writer lock (DrainOnce), which serializes against the
// daemon and any other concurrent sender — so no queued message is ever lost.
func (c *Client) Send(content string) error {
	_ = c.Layout.Heartbeat(c.Agent)
	if err := c.Layout.Enqueue(c.Agent, content); err != nil {
		return err
	}
	if err := c.EnsureWriter(); err != nil {
		return err
	}
	if WriterRunning(c.Layout) {
		return nil // daemon will drain the queue
	}
	// No daemon could be started: drain the queue ourselves, safely.
	if _, err := DrainOnce(c.Layout); err != nil {
		return err
	}
	return nil
}

// Poll returns all messages newer than this agent's cursor and advances the
// cursor. It only reads the message DB and reads/writes its own cursor file —
// never writing the message table, so there is zero poll-side lock contention.
func (c *Client) Poll() ([]Message, error) {
	_ = c.Layout.Heartbeat(c.Agent)
	store, err := OpenStore(c.Layout.DB)
	if err != nil {
		return nil, err
	}
	defer store.Close()

	cur, err := c.Layout.ReadCursor(c.Agent)
	if err != nil {
		return nil, err
	}
	msgs, err := store.Since(cur)
	if err != nil {
		return nil, err
	}
	if len(msgs) > 0 {
		last := msgs[len(msgs)-1].ID
		if err := c.Layout.WriteCursor(c.Agent, last); err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

// Wait blocks until this agent is @mentioned (or @all) or the timeout elapses,
// then returns all new messages since the agent's cursor. A zero timeout waits
// indefinitely.
//
// Semantics: a wakeup happens only on a mention of this agent (or @all). Before
// blocking, Wait peeks at messages that arrived since its cursor and, if any of
// them mention this agent, returns immediately. This closes the race where a
// mention lands in the gap between two Wait calls (after the previous wakeup,
// before the inotify watch is re-armed) without waking the agent for unrelated
// chatter. On wakeup the agent receives all new messages for full context, not
// just the mentioning line.
func (c *Client) Wait(timeout time.Duration) ([]Message, bool, error) {
	_ = c.Layout.Heartbeat(c.Agent)
	if err := c.Layout.EnsureNotifyFiles(c.Agent); err != nil {
		return nil, false, err
	}
	// Mention-aware catch-up: only short-circuit if a pending message actually
	// mentions this agent. Peek without advancing the cursor.
	if mentioned, err := c.hasPendingMention(); err != nil {
		return nil, false, err
	} else if mentioned {
		msgs, err := c.Poll()
		return msgs, true, err
	}
	// Keep presence fresh while blocked so a legitimately-waiting agent shows
	// as online/sleeping rather than decaying to offline.
	stopHB := make(chan struct{})
	go func() {
		t := time.NewTicker(OnlineWindow / 2)
		defer t.Stop()
		for {
			select {
			case <-stopHB:
				return
			case <-t.C:
				_ = c.Layout.Heartbeat(c.Agent)
			}
		}
	}()
	woken, err := c.Layout.WaitForMention(c.Agent, timeout)
	close(stopHB)
	if err != nil {
		return nil, false, err
	}
	if !woken {
		// Timed out with no mention: consume nothing, leave the cursor intact.
		return nil, false, nil
	}
	msgs, err := c.Poll()
	return msgs, woken, err
}

// hasPendingMention reports whether any message newer than this agent's cursor
// mentions the agent (by name or @all). It does not advance the cursor.
func (c *Client) hasPendingMention() (bool, error) {
	store, err := OpenStore(c.Layout.DB)
	if err != nil {
		return false, err
	}
	defer store.Close()
	roster, err := LoadRoster(c.Layout.Roster)
	if err != nil {
		return false, err
	}
	cur, err := c.Layout.ReadCursor(c.Agent)
	if err != nil {
		return false, err
	}
	msgs, err := store.Since(cur)
	if err != nil {
		return false, err
	}
	self := strings.ToLower(c.Agent)
	for _, m := range msgs {
		for _, target := range ParseMentions(m.Content, roster) {
			if target == "all" || target == self {
				return true, nil
			}
		}
	}
	return false, nil
}

// History returns up to the last n messages, oldest first. It does not advance
// any cursor — it is a read-only view for humans.
func (c *Client) History(n int) ([]Message, error) {
	store, err := OpenStore(c.Layout.DB)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	return store.Recent(n)
}

// Watch live-tails every new message (all traffic, not just mentions) and calls
// emit for each, until stop is closed. It tracks its own in-memory last-seen id
// so it never touches the agent cursor. Delivery is driven by the shared "all"
// notify file plus a short poll tick that also catches directed messages.
func (c *Client) Watch(stop <-chan struct{}, emit func(Message)) error {
	store, err := OpenStore(c.Layout.DB)
	if err != nil {
		return err
	}
	defer store.Close()

	last, err := store.MaxID()
	if err != nil {
		return err
	}
	if err := c.Layout.EnsureNotifyFiles("all"); err != nil {
		return err
	}

	for {
		select {
		case <-stop:
			return nil
		default:
		}
		// Wake on an @all poke, but cap the block so directed messages (which
		// poke only the target) are still picked up by the periodic re-poll.
		_, _ = c.Layout.WaitForMention("all", 1*time.Second)

		msgs, err := store.Since(last)
		if err != nil {
			return err
		}
		for _, m := range msgs {
			emit(m)
			last = m.ID
		}
	}
}

// AgentStatus describes one roster member's liveness.
type AgentStatus struct {
	Name     string   `json:"name"`
	Cursor   int64    `json:"cursor"`
	Liveness Liveness `json:"liveness"`
}

// Status reports the writer daemon state and each roster member's cursor and
// presence (online/sleeping/offline).
func (c *Client) Status() (writerUp bool, agents []AgentStatus, err error) {
	writerUp = WriterRunning(c.Layout)
	roster, err := LoadRoster(c.Layout.Roster)
	if err != nil {
		return writerUp, nil, err
	}
	for _, name := range roster.Names() {
		cur, _ := c.Layout.ReadCursor(name)
		agents = append(agents, AgentStatus{
			Name:     name,
			Cursor:   cur,
			Liveness: c.Layout.LivenessOf(name),
		})
	}
	return writerUp, agents, nil
}

// Go is the agent main-loop primitive behind spark_go. It optionally sends a
// reply first, then blocks until the next @mention (or @all) and returns the
// new messages. A zero timeout (or "sleep") waits indefinitely, only ever
// woken by a mention — the deep-sleep mode that costs nothing until pinged.
func (c *Client) Go(reply string, timeout time.Duration) ([]Message, bool, error) {
	if strings.TrimSpace(reply) != "" {
		if err := c.Send(reply); err != nil {
			return nil, false, err
		}
	}
	return c.Wait(timeout)
}

// String renders a message for human-readable tool output.
func (m Message) String() string {
	return fmt.Sprintf("[#%d %s] %s: %s",
		m.ID, time.UnixMilli(m.TS).Format("15:04:05"), m.Sender, m.Content)
}
