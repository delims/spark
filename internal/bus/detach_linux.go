package bus

import "syscall"

// detachAttr returns SysProcAttr that puts the autostarted writer daemon in a
// new session, detached from the parent agent's controlling terminal.
func detachAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
