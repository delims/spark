package bus

import (
	"encoding/json"
	"os"

	"golang.org/x/sys/unix"
)

// QueueEntry is one line in the append-only write_queue. Each entry is a
// single JSON object so that content containing spaces, newlines or '@' never
// corrupts the delimiter. The writer daemon consumes these lines serially.
type QueueEntry struct {
	Sender  string `json:"sender"`
	Content string `json:"content"`
}

// Enqueue appends a message to the write queue as one JSON line. The append is
// guarded by a short-held exclusive flock on the queue file so it can never
// interleave with the writer's compaction step. This lock protects only the
// tiny queue append (microseconds); the message DB itself is never touched by
// senders, preserving the lock-free DB guarantee.
func (l *Layout) Enqueue(sender, content string) error {
	entry := QueueEntry{Sender: sender, Content: content}
	line, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	f, err := os.OpenFile(l.Queue, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		return err
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN) //nolint:errcheck
	_, err = f.Write(line)
	return err
}
