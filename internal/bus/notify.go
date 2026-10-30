package bus

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Poke touches a notify file, waking any inotify watcher on it. Writing a byte
// (then truncating) reliably generates an IN_MODIFY/IN_CLOSE_WRITE event.
func (l *Layout) Poke(lowerName string) error {
	path := l.NotifyFile(lowerName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte{'.'})
	return err
}

// EnsureNotifyFiles makes sure the agent's own notify file and the shared
// "all" file exist before we start watching them.
func (l *Layout) EnsureNotifyFiles(agent string) error {
	for _, name := range []string{agent, "all"} {
		p := l.NotifyFile(name)
		f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		f.Close()
	}
	return nil
}

// WaitForMention blocks until the agent's own notify file or the shared "all"
// file is modified, or until the timeout elapses. It returns true if woken by
// a notify event, false on timeout. A zero or negative timeout waits forever.
//
// This is the event-driven core: with no @mention nobody poking the files,
// nobody wakes — zero polling, zero wasted work.
func (l *Layout) WaitForMention(agent string, timeout time.Duration) (bool, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC)
	if err != nil {
		return false, err
	}
	defer unix.Close(fd)

	for _, name := range []string{agent, "all"} {
		// Watch IN_MODIFY only. A wakeup must correspond to an actual byte
		// written by Poke; IN_CLOSE_WRITE would also fire when any process
		// merely opens-for-write and closes the file (e.g. EnsureNotifyFiles),
		// causing spurious wakeups for agents that were never mentioned.
		if _, err := unix.InotifyAddWatch(fd, l.NotifyFile(name),
			unix.IN_MODIFY); err != nil {
			return false, err
		}
	}

	// Use poll(2) to apply the timeout to the blocking inotify read.
	pfd := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	timeoutMs := -1
	if timeout > 0 {
		timeoutMs = int(timeout.Milliseconds())
	}

	for {
		n, err := unix.Poll(pfd, timeoutMs)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return false, err
		}
		if n == 0 {
			return false, nil // timeout
		}
		// Drain the event buffer so the fd is left clean.
		var buf [4096]byte
		if _, err := unix.Read(fd, buf[:]); err != nil && err != unix.EAGAIN {
			return false, err
		}
		return true, nil
	}
}
