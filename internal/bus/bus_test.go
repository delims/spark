package bus

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

func testRoster() *Roster {
	return &Roster{members: map[string]string{
		"zara": "Zara", "nora": "Nora", "cora": "Cora",
	}}
}

func TestParseMentions(t *testing.T) {
	r := testRoster()
	cases := []struct {
		in   string
		want []string
	}{
		{"@Zara hello", []string{"zara"}},
		{"hey @Nora, how are you?", []string{"nora"}},
		{"@all standup now", []string{"all"}},
		{"no mentions here", nil},
		{"@Zara @Nora both of you", []string{"zara", "nora"}},
		{"@Zara @Zara dedup", []string{"zara"}},
		{"@Unknown not in roster", nil},
		{"email me@example.com is not a mention", nil},
		{"trailing dot @Cora.", []string{"cora"}},
		{"case @ZARA insensitive", []string{"zara"}},
	}
	for _, c := range cases {
		got := ParseMentions(c.in, r)
		if !equal(got, c.want) {
			t.Errorf("ParseMentions(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestStoreCursorFlow(t *testing.T) {
	dir := t.TempDir()
	l, err := NewLayout(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(l.DB)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	id1, err := st.Insert("Zara", "first")
	if err != nil {
		t.Fatal(err)
	}
	id2, _ := st.Insert("Nora", "@Zara second")

	// Nora's fresh cursor (0) should see both.
	cur, _ := l.ReadCursor("nora")
	msgs, _ := st.Since(cur)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	_ = l.WriteCursor("nora", msgs[len(msgs)-1].ID)

	// After advancing, no new messages.
	cur, _ = l.ReadCursor("nora")
	if cur != id2 {
		t.Fatalf("cursor = %d, want %d", cur, id2)
	}
	msgs, _ = st.Since(cur)
	if len(msgs) != 0 {
		t.Fatalf("expected 0 new messages, got %d", len(msgs))
	}

	max, _ := st.MaxID()
	if max != id2 || id1 >= id2 {
		t.Fatalf("id ordering broken: id1=%d id2=%d max=%d", id1, id2, max)
	}
}

func TestWaitWakesOnPoke(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLayout(dir)
	_ = l.EnsureDirs()
	_ = l.EnsureNotifyFiles("zara")

	woken := make(chan bool, 1)
	go func() {
		w, err := l.WaitForMention("zara", 3*time.Second)
		if err != nil {
			t.Errorf("wait error: %v", err)
		}
		woken <- w
	}()

	time.Sleep(200 * time.Millisecond)
	if err := l.Poke("zara"); err != nil {
		t.Fatal(err)
	}

	select {
	case w := <-woken:
		if !w {
			t.Fatal("expected to be woken, got timeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("WaitForMention did not return after poke")
	}
}

func TestWaitTimeout(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLayout(dir)
	_ = l.EnsureDirs()
	_ = l.EnsureNotifyFiles("nora")

	start := time.Now()
	woken, err := l.WaitForMention("nora", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if woken {
		t.Fatal("expected timeout, got wakeup")
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("returned too early")
	}
}

// newTestClient wires a client to an isolated base with a Zara/Nora/Cora roster.
func newTestClient(t *testing.T, base, agent string) *Client {
	t.Helper()
	if err := os.WriteFile(base+"/roster.conf", []byte("Zara\nNora\nCora\n"), 0o644); err != nil {
		// base/roster may not exist yet; ensure dir first
	}
	c, err := NewClient(agent, base)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(c.Layout.Roster, []byte("Zara\nNora\nCora\n"), 0o644)
	return c
}

// TestWaitNoSpuriousWake: an un-mentioned message must not wake a waiter.
func TestWaitNoSpuriousWake(t *testing.T) {
	dir := t.TempDir()
	nora := newTestClient(t, dir, "Nora")
	zara := newTestClient(t, dir, "Zara")

	// Insert an un-mentioned message directly (bypass daemon).
	st, _ := OpenStore(zara.Layout.DB)
	_, _ = st.Insert("Zara", "hello everyone but no mention")
	st.Close()

	start := time.Now()
	msgs, woken, err := nora.Wait(300 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if woken {
		t.Fatalf("Nora should not be woken by an un-mentioned message, got msgs=%v", msgs)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected no messages on timeout, got %d", len(msgs))
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("returned too early — did not actually block")
	}
}

// TestWaitCatchUpOnPendingMention: a mention already in the DB is returned
// immediately without blocking.
func TestWaitCatchUpOnPendingMention(t *testing.T) {
	dir := t.TempDir()
	nora := newTestClient(t, dir, "Nora")

	st, _ := OpenStore(nora.Layout.DB)
	_, _ = st.Insert("Zara", "@Nora catch this")
	st.Close()

	start := time.Now()
	msgs, woken, err := nora.Wait(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !woken || len(msgs) != 1 {
		t.Fatalf("expected immediate catch-up with 1 msg, got woken=%v msgs=%d", woken, len(msgs))
	}
	if time.Since(start) > time.Second {
		t.Fatal("catch-up should return immediately, not block")
	}
}

// TestConcurrentSendNoLoss fires many Send calls in parallel against a base
// where the writer daemon cannot run (its autostart is a no-op in tests since
// os.Executable points at the test binary). Every queued message must still be
// persisted exactly once via the DrainOnce fallback — no loss, no duplicates.
func TestConcurrentSendNoLoss(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(dir+"/roster.conf", []byte("Zara\nNora\nCora\n"), 0o644)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := NewClient("Zara", dir)
			if err != nil {
				t.Errorf("client: %v", err)
				return
			}
			// Enqueue + safe drain without relying on a daemon.
			if err := c.Layout.Enqueue("Zara", fmt.Sprintf("m-%d", i)); err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			if _, err := DrainOnce(c.Layout); err != nil {
				t.Errorf("drain: %v", err)
			}
		}(i)
	}
	wg.Wait()

	// Final sweep in case the last enqueue landed after the last drainer left.
	l, _ := NewLayout(dir)
	for i := 0; i < 5; i++ {
		if _, err := DrainOnce(l); err != nil {
			t.Fatal(err)
		}
	}

	st, _ := OpenStore(l.DB)
	defer st.Close()
	msgs, _ := st.Since(0)
	if len(msgs) != n {
		t.Fatalf("expected %d messages, got %d (loss or duplication)", n, len(msgs))
	}
	seen := map[string]int{}
	for _, m := range msgs {
		seen[m.Content]++
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("m-%d", i)
		if seen[k] != 1 {
			t.Fatalf("message %q appeared %d times, want 1", k, seen[k])
		}
	}
}
func TestWaitAllWakesEveryone(t *testing.T) {
	dir := t.TempDir()
	cora := newTestClient(t, dir, "Cora")

	st, _ := OpenStore(cora.Layout.DB)
	_, _ = st.Insert("Zara", "@all broadcast")
	st.Close()

	msgs, woken, err := cora.Wait(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !woken || len(msgs) != 1 {
		t.Fatalf("@all should wake Cora, got woken=%v msgs=%d", woken, len(msgs))
	}
}
