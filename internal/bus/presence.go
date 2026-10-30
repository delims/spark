package bus

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Presence thresholds. An agent that has touched its presence file within
// OnlineWindow is "online"; within SleepingWindow it is "sleeping" (idle but
// alive); older than that (or never seen) it is "offline".
const (
	OnlineWindow   = 30 * time.Second
	SleepingWindow = 5 * time.Minute
)

// Liveness is a coarse presence state for an agent.
type Liveness string

const (
	Online   Liveness = "online"
	Sleeping Liveness = "sleeping"
	Offline  Liveness = "offline"
)

// PresenceDir returns the presence directory (lazily, derived from Base).
func (l *Layout) presenceDir() string { return filepath.Join(l.Base, "presence") }

// presenceFile is the per-agent heartbeat file.
func (l *Layout) presenceFile(agent string) string {
	return filepath.Join(l.presenceDir(), strings.ToLower(agent))
}

// Heartbeat records that the given agent is alive right now by writing the
// current unix-millis timestamp to its presence file.
func (l *Layout) Heartbeat(agent string) error {
	if err := os.MkdirAll(l.presenceDir(), 0o755); err != nil {
		return err
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	tmp := l.presenceFile(agent) + ".tmp"
	if err := os.WriteFile(tmp, []byte(ts), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, l.presenceFile(agent))
}

// LivenessOf returns the coarse presence state for an agent based on the age
// of its heartbeat file.
func (l *Layout) LivenessOf(agent string) Liveness {
	b, err := os.ReadFile(l.presenceFile(agent))
	if err != nil {
		return Offline
	}
	ms, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64)
	if err != nil {
		return Offline
	}
	age := time.Since(time.UnixMilli(ms))
	switch {
	case age <= OnlineWindow:
		return Online
	case age <= SleepingWindow:
		return Sleeping
	default:
		return Offline
	}
}
