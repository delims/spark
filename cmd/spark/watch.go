package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/delims/spark/internal/bus"
)

// cmdWatch live-tails the conversation. It prints existing history once, then
// blocks and prints every new message as it is stored. Unlike an agent's
// spark_wait (which only wakes on @mentions), a human watcher sees all traffic.
func cmdWatch(args []string) {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	as := fs.String("as", "", "watcher display name")
	n := fs.Int("n", 20, "how many recent messages to show before tailing")
	_ = fs.Parse(args)

	c := newClient(*as)

	// Print recent history for context.
	hist, err := c.History(*n)
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark watch:", err)
		os.Exit(1)
	}
	for _, m := range hist {
		fmt.Println(m.String())
	}
	fmt.Printf("── watching as %s · Ctrl-C to quit ──\n", c.Agent)

	// Graceful Ctrl-C.
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	stop := make(chan struct{})
	go func() {
		<-sigc
		close(stop)
	}()

	// Live tail: block for new messages, print them, repeat.
	err = c.Watch(stop, func(m bus.Message) {
		fmt.Println(m.String())
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "\nspark watch:", err)
		os.Exit(1)
	}
	fmt.Println("\n── stopped ──")
}
