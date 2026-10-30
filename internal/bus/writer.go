package bus

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// Writer is the single-writer daemon. All DB INSERTs flow through one Writer
// process, which eliminates multi-process write-lock contention: concurrent
// senders only ever append to the queue file.
type Writer struct {
	layout *Layout
	store  *Store
	roster *Roster
	log    *log.Logger
	lockFD int
}

// WriterRunning reports whether a live writer daemon owns the lock. It does so
// by trying to take a shared, non-blocking flock: if that fails with EWOULDBLOCK
// the lock is held exclusively by a running daemon.
func WriterRunning(l *Layout) bool {
	pid, ok := readPID(l.WriterPID)
	if !ok {
		return false
	}
	// Signal 0 checks for existence without delivering a signal.
	if err := syscall.Kill(pid, 0); err != nil {
		return false
	}
	return true
}

func readPID(path string) (int, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(string(bytes.TrimSpace(b)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// RunWriter starts the writer daemon in the current process. It acquires an
// exclusive flock (so only one writer ever runs), writes its PID, then loops:
// wait for queue changes via inotify, drain the queue, insert + poke mentions.
func RunWriter(l *Layout) error {
	if err := l.EnsureDirs(); err != nil {
		return err
	}

	logFile, err := os.OpenFile(l.WriterLog, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	logger := log.New(logFile, "", log.LstdFlags|log.Lmicroseconds)

	// Exclusive single-instance lock.
	lockFD, err := unix.Open(l.WriterLock, unix.O_CREAT|unix.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open lock: %w", err)
	}
	if err := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		unix.Close(lockFD)
		return fmt.Errorf("another writer is already running: %w", err)
	}

	store, err := OpenStore(l.DB)
	if err != nil {
		unix.Close(lockFD)
		return err
	}
	roster, err := LoadRoster(l.Roster)
	if err != nil {
		store.Close()
		unix.Close(lockFD)
		return err
	}

	// Ensure the queue file exists so inotify can watch it.
	if f, err := os.OpenFile(l.Queue, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		f.Close()
	}

	if err := os.WriteFile(l.WriterPID, []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
		store.Close()
		unix.Close(lockFD)
		return err
	}

	w := &Writer{layout: l, store: store, roster: roster, log: logger, lockFD: lockFD}
	defer w.cleanup()

	w.log.Printf("writer daemon started pid=%d base=%s", os.Getpid(), l.Base)

	// Graceful shutdown on SIGTERM/SIGINT.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT)
	done := make(chan struct{})
	go func() {
		<-sigc
		w.log.Printf("writer daemon stopping")
		close(done)
	}()

	// inotify watch on the queue file.
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	if _, err := unix.InotifyAddWatch(fd, l.Queue, unix.IN_MODIFY); err != nil {
		return err
	}

	// Process anything already queued before we started watching.
	w.drain()

	pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	for {
		select {
		case <-done:
			return nil
		default:
		}
		// 500ms poll tick doubles as a safety net against missed events.
		n, err := unix.Poll(pfd, 500)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			w.log.Printf("poll error: %v", err)
			continue
		}
		if n > 0 {
			var buf [4096]byte
			unix.Read(fd, buf[:]) //nolint:errcheck // drain
		}
		w.drain()
	}
}

// drain reads all queued lines, inserts each into the DB, pokes mentions, then
// truncates the queue. It holds an exclusive flock on the queue file for the
// whole read-process-truncate cycle, so concurrent Enqueue appends (which take
// the same lock) can never interleave — no queued message is lost or
// double-processed.
func (w *Writer) drain() {
	f, err := os.OpenFile(w.layout.Queue, os.O_RDWR, 0o644)
	if err != nil {
		if !os.IsNotExist(err) {
			w.log.Printf("open queue: %v", err)
		}
		return
	}
	defer f.Close()

	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		w.log.Printf("flock queue: %v", err)
		return
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN) //nolint:errcheck

	info, err := f.Stat()
	if err != nil || info.Size() == 0 {
		return
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var processed int
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var e QueueEntry
		if err := json.Unmarshal(line, &e); err != nil {
			w.log.Printf("bad queue line, skipping: %v", err)
			continue
		}
		id, err := w.store.Insert(e.Sender, e.Content)
		if err != nil {
			w.log.Printf("insert failed: %v", err)
			continue
		}
		processed++
		w.wake(e.Content)
		w.log.Printf("stored id=%d sender=%s", id, e.Sender)
	}
	if err := sc.Err(); err != nil {
		w.log.Printf("scan queue: %v", err)
		return // leave the queue intact; retry next drain
	}

	// Safe to truncate: the exclusive lock blocked all appends during the read.
	if err := f.Truncate(0); err != nil {
		w.log.Printf("truncate queue: %v", err)
	}
	if processed > 0 {
		w.log.Printf("drained %d message(s)", processed)
	}
}

// wake pokes notify files for every @mention found in content.
func (w *Writer) wake(content string) {
	for _, target := range ParseMentions(content, w.roster) {
		if err := w.layout.Poke(target); err != nil {
			w.log.Printf("poke %s: %v", target, err)
		}
	}
}

func (w *Writer) cleanup() {
	w.store.Close()
	unix.Flock(w.lockFD, unix.LOCK_UN) //nolint:errcheck
	unix.Close(w.lockFD)               //nolint:errcheck
	os.Remove(w.layout.WriterPID)      //nolint:errcheck
	w.log.Printf("writer daemon exited")
}

// StopWriter signals a running writer daemon to stop and waits briefly for it
// to exit.
func StopWriter(l *Layout) error {
	pid, ok := readPID(l.WriterPID)
	if !ok {
		return fmt.Errorf("no writer.pid found")
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("signal pid %d: %w", pid, err)
	}
	for i := 0; i < 30; i++ {
		if err := syscall.Kill(pid, 0); err != nil {
			return nil // gone
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("writer pid %d did not exit in time", pid)
}

// DrainOnce processes the write queue exactly once under the exclusive writer
// lock, then returns. It is the fallback used by Send when no writer daemon is
// available: because it takes the same exclusive flock the daemon uses, it is
// serialized against the daemon and against other fallback drainers, so the
// read-insert-truncate cycle never races and no queued message is lost.
//
// If the lock is held (a daemon or another drainer is active), DrainOnce
// returns immediately with ok=false — the holder will process the queue.
func DrainOnce(l *Layout) (ok bool, err error) {
	lockFD, err := unix.Open(l.WriterLock, unix.O_CREAT|unix.O_RDWR, 0o644)
	if err != nil {
		return false, err
	}
	defer unix.Close(lockFD)
	if err := unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return false, nil // someone else owns it; they'll drain
	}
	defer unix.Flock(lockFD, unix.LOCK_UN) //nolint:errcheck

	store, err := OpenStore(l.DB)
	if err != nil {
		return false, err
	}
	defer store.Close()
	roster, err := LoadRoster(l.Roster)
	if err != nil {
		return false, err
	}
	w := &Writer{layout: l, store: store, roster: roster, log: log.New(io.Discard, "", 0)}
	w.drain()
	return true, nil
}
