// Command spark is the human-facing CLI for the Spark message bus. While
// spark-mcp exposes the bus to AI agents over MCP, this binary gives a person a
// way to send messages and watch the conversation from a terminal.
//
// Usage:
//
//	spark send [-as NAME] "<message>"   send a message (@Name / @all to notify)
//	spark watch [-as NAME]              live-tail new messages (inotify-driven)
//	spark status                        show writer state + presence + cursors
//	spark history [-n N]                print the last N messages and exit
//
// Environment:
//
//	SPARK_AGENT  default display name for -as (default: "You")
//	SPARK_BASE   bus root directory (default: ~/.config/spark)
//	SPARK_KEY    optional key enabling encrypted-at-rest content
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/delims/spark/internal/bus"
)

func main() {
	if len(os.Args) < 2 {
		// Bare `spark` drops you straight into the chat UI — one command online.
		cmdTUI(nil)
		return
	}
	cmd := os.Args[1]
	args := os.Args[2:]

	// A leading flag (e.g. `spark -as Alice`) means "open the TUI with these
	// flags", not a subcommand. Only -h/--help short-circuit to usage.
	if strings.HasPrefix(cmd, "-") {
		if cmd == "-h" || cmd == "--help" {
			usage()
			return
		}
		cmdTUI(os.Args[1:])
		return
	}

	switch cmd {
	case "send":
		cmdSend(args)
	case "watch":
		cmdWatch(args)
	case "tui", "chat":
		cmdTUI(args)
	case "status":
		cmdStatus(args)
	case "history":
		cmdHistory(args)
	case "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `spark — human CLI for the Spark message bus

Usage:
  spark [-as NAME]                    Open the full-screen chat UI (default).
  spark tui [-as NAME]                Same as above (alias: chat).
  spark send [-as NAME] "<message>"   Send a message. Use @Name or @all to notify.
  spark watch [-as NAME]              Live-tail new messages (press Ctrl-C to quit).
  spark status                        Show writer state, presence and cursors.
  spark history [-n N]                Print the last N messages (default 20) and exit.

Environment:
  SPARK_AGENT  default sender name (default "You")
  SPARK_BASE   bus directory (default ~/.config/spark)
  SPARK_KEY    optional encryption key
`)
}

// newClient builds a bus client using -as / SPARK_AGENT for the display name.
func newClient(as string) *bus.Client {
	name := as
	if name == "" {
		name = os.Getenv("SPARK_AGENT")
	}
	if name == "" {
		name = "You"
	}
	c, err := bus.NewClient(name, os.Getenv("SPARK_BASE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark:", err)
		os.Exit(1)
	}
	return c
}

func cmdSend(args []string) {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	as := fs.String("as", "", "sender display name")
	_ = fs.Parse(args)

	msg := strings.TrimSpace(strings.Join(fs.Args(), " "))
	if msg == "" {
		fmt.Fprintln(os.Stderr, "spark send: message is empty")
		os.Exit(2)
	}
	c := newClient(*as)
	if err := c.Send(msg); err != nil {
		fmt.Fprintln(os.Stderr, "spark send:", err)
		os.Exit(1)
	}
	fmt.Printf("sent as %s: %s\n", c.Agent, msg)
}

func cmdStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	as := fs.String("as", "", "observer display name")
	_ = fs.Parse(args)

	c := newClient(*as)
	up, agents, err := c.Status()
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark status:", err)
		os.Exit(1)
	}
	state := "down"
	if up {
		state = "running"
	}
	fmt.Printf("writer daemon: %s\n", state)
	fmt.Println("roster:")
	for _, a := range agents {
		fmt.Printf("  - %-12s %-9s cursor=%d\n", a.Name, a.Liveness, a.Cursor)
	}
}

func cmdHistory(args []string) {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	as := fs.String("as", "", "observer display name")
	n := fs.Int("n", 20, "number of recent messages to show")
	_ = fs.Parse(args)

	c := newClient(*as)
	msgs, err := c.History(*n)
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark history:", err)
		os.Exit(1)
	}
	if len(msgs) == 0 {
		fmt.Println("(no messages yet)")
		return
	}
	for _, m := range msgs {
		fmt.Println(m.String())
	}
}
