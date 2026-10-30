// Package bus implements the Spark message bus: a lock-free, event-driven
// multi-agent message bus backed by SQLite (WAL), a single-writer daemon that
// serializes all writes via an append-only queue, and inotify-based @mention
// wakeups. See REQUIREMENTS.md for the design rationale.
package bus

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Layout holds all runtime paths derived from SPARK_BASE.
type Layout struct {
	Base       string // root dir (e.g. ~/.config/spark)
	DB         string // chat.db
	Queue      string // write_queue (append-only)
	WriterPID  string // writer.pid
	WriterLock string // writer.lock
	WriterLog  string // writer.log
	Roster     string // roster.conf
	NotifyDir  string // notify/
	CursorDir  string // cursors/
}

// NewLayout expands SPARK_BASE (with ~ support) and returns the path layout.
// If base is empty it defaults to ~/.config/spark.
func NewLayout(base string) (*Layout, error) {
	if base == "" {
		base = "~/.config/spark"
	}
	expanded, err := expandHome(base)
	if err != nil {
		return nil, err
	}
	l := &Layout{
		Base:       expanded,
		DB:         filepath.Join(expanded, "chat.db"),
		Queue:      filepath.Join(expanded, "write_queue"),
		WriterPID:  filepath.Join(expanded, "writer.pid"),
		WriterLock: filepath.Join(expanded, "writer.lock"),
		WriterLog:  filepath.Join(expanded, "writer.log"),
		Roster:     filepath.Join(expanded, "roster.conf"),
		NotifyDir:  filepath.Join(expanded, "notify"),
		CursorDir:  filepath.Join(expanded, "cursors"),
	}
	return l, nil
}

// EnsureDirs creates the base, notify and cursor directories.
func (l *Layout) EnsureDirs() error {
	for _, d := range []string{l.Base, l.NotifyDir, l.CursorDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", d, err)
		}
	}
	return nil
}

// NotifyFile returns the notify file path for a given lowercase name.
func (l *Layout) NotifyFile(name string) string {
	return filepath.Join(l.NotifyDir, strings.ToLower(name))
}

// CursorFile returns the per-agent cursor file path.
func (l *Layout) CursorFile(agent string) string {
	return filepath.Join(l.CursorDir, strings.ToLower(agent))
}

func expandHome(p string) (string, error) {
	if p == "~" || strings.HasPrefix(p, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if p == "~" {
			return home, nil
		}
		return filepath.Join(home, p[2:]), nil
	}
	return p, nil
}
