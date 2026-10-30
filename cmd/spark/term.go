package main

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// term wraps a terminal in raw mode on the alternate screen buffer, restoring
// the original state on close. It uses only x/sys/unix — no TUI framework — to
// stay consistent with the project's from-first-principles approach.
type term struct {
	fd   int
	prev *unix.Termios
}

// enterRaw switches stdin to raw mode and the terminal to the alternate screen
// (so the chat UI doesn't clobber the user's scrollback).
func enterRaw() (*term, error) {
	fd := int(os.Stdin.Fd())
	prev, err := unix.IoctlGetTermios(fd, unix.TCGETS)
	if err != nil {
		return nil, err
	}
	raw := *prev
	// Disable canonical mode, echo, signals and flow control so we get keys
	// one at a time without the terminal interpreting them.
	raw.Lflag &^= unix.ICANON | unix.ECHO | unix.ISIG | unix.IEXTEN
	raw.Iflag &^= unix.IXON | unix.ICRNL | unix.BRKINT | unix.INPCK | unix.ISTRIP
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &raw); err != nil {
		return nil, err
	}
	// Enter alternate screen, hide cursor during redraws.
	os.Stdout.WriteString("\x1b[?1049h")
	return &term{fd: fd, prev: prev}, nil
}

// close restores the original terminal state and leaves the alternate screen.
func (t *term) close() {
	os.Stdout.WriteString("\x1b[?1049l") // leave alt screen
	os.Stdout.WriteString("\x1b[?25h")   // show cursor
	if t.prev != nil {
		_ = unix.IoctlSetTermios(t.fd, unix.TCSETS, t.prev)
	}
}

// size returns the current terminal dimensions (rows, cols).
func (t *term) size() (rows, cols int) {
	ws, err := unix.IoctlGetWinsize(t.fd, unix.TIOCGWINSZ)
	if err != nil || ws.Row == 0 {
		return 24, 80
	}
	return int(ws.Row), int(ws.Col)
}

// readBlocking reads from the terminal fd, blocking until at least one byte is
// available (VMIN=1). Returns the number of bytes read, or <=0 on error.
func (t *term) readBlocking(p []byte) int {
	n, err := unix.Read(t.fd, p)
	if err != nil {
		if err == unix.EINTR {
			return t.readBlocking(p)
		}
		return -1
	}
	return n
}

// readTimeout reads available bytes into p, waiting at most d for the first
// byte. It returns 0 on timeout. Used to tell a lone Esc (quit) from the start
// of an arrow-key escape sequence (ESC [ A …), whose remaining bytes arrive
// within a few milliseconds.
func (t *term) readTimeout(p []byte, d time.Duration) int {
	fds := []unix.PollFd{{Fd: int32(t.fd), Events: unix.POLLIN}}
	n, err := unix.Poll(fds, int(d.Milliseconds()))
	if err != nil || n == 0 {
		return 0
	}
	m, err := unix.Read(t.fd, p)
	if err != nil || m < 0 {
		return 0
	}
	return m
}
