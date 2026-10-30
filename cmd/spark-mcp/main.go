// Command spark-mcp is the Spark message-bus MCP server. It speaks JSON-RPC 2.0
// over stdio (hand-written, no SDK) and exposes the bus as MCP tools. It also
// hosts the internal `writer` subcommand used to run the single-writer daemon.
//
// Environment:
//
//	SPARK_AGENT  this agent's display name (default: anon)
//	SPARK_BASE   bus root directory       (default: ~/.config/spark)
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/delims/spark/internal/bus"
)

const protocolVersion = "2024-11-05"

func main() {
	// Internal subcommand: run the writer daemon in this process.
	if len(os.Args) > 1 && os.Args[1] == "writer" {
		l, err := bus.NewLayout(os.Getenv("SPARK_BASE"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "writer:", err)
			os.Exit(1)
		}
		if err := bus.RunWriter(l); err != nil {
			fmt.Fprintln(os.Stderr, "writer:", err)
			os.Exit(1)
		}
		return
	}

	client, err := bus.NewClient(os.Getenv("SPARK_AGENT"), os.Getenv("SPARK_BASE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark-mcp init:", err)
		os.Exit(1)
	}

	srv := &server{client: client, out: bufio.NewWriter(os.Stdout)}
	srv.serve(os.Stdin)
}

// ---- JSON-RPC 2.0 framing (newline-delimited JSON over stdio) ----

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type server struct {
	client *bus.Client
	out    *bufio.Writer
}

func (s *server) serve(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			continue // ignore unparseable frames
		}
		s.handle(&req)
	}
}

func (s *server) handle(req *rpcRequest) {
	switch req.Method {
	case "initialize":
		s.reply(req.ID, map[string]interface{}{
			"protocolVersion": protocolVersion,
			"capabilities":    map[string]interface{}{"tools": map[string]interface{}{}},
			"serverInfo":      map[string]interface{}{"name": "spark", "version": "1.0.0"},
		})
	case "notifications/initialized":
		// notification, no response
	case "tools/list":
		s.reply(req.ID, map[string]interface{}{"tools": toolDefs()})
	case "tools/call":
		s.handleToolCall(req)
	case "ping":
		s.reply(req.ID, map[string]interface{}{})
	default:
		if len(req.ID) > 0 {
			s.replyErr(req.ID, -32601, "method not found: "+req.Method)
		}
	}
}

func (s *server) reply(id json.RawMessage, result interface{}) {
	if len(id) == 0 {
		return // notification
	}
	s.write(rpcResponse{JSONRPC: "2.0", ID: id, Result: result})
}

func (s *server) replyErr(id json.RawMessage, code int, msg string) {
	s.write(rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}})
}

func (s *server) write(resp rpcResponse) {
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	s.out.Write(b)
	s.out.WriteByte('\n')
	s.out.Flush()
}

// ---- MCP tool helpers ----

// toolResult builds a standard MCP text content result.
func toolResult(text string) map[string]interface{} {
	return map[string]interface{}{
		"content": []map[string]interface{}{{"type": "text", "text": text}},
	}
}

func (s *server) handleToolCall(req *rpcRequest) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		s.replyErr(req.ID, -32602, "invalid params")
		return
	}
	args := map[string]interface{}{}
	if len(params.Arguments) > 0 {
		_ = json.Unmarshal(params.Arguments, &args)
	}

	switch params.Name {
	case "spark_send":
		s.toolSend(req.ID, args)
	case "spark_poll":
		s.toolPoll(req.ID)
	case "spark_wait":
		s.toolWait(req.ID, args)
	case "spark_go":
		s.toolGo(req.ID, args)
	case "spark_status":
		s.toolStatus(req.ID)
	default:
		s.replyErr(req.ID, -32601, "unknown tool: "+params.Name)
	}
}

func (s *server) toolSend(id json.RawMessage, args map[string]interface{}) {
	msg, _ := args["message"].(string)
	if strings.TrimSpace(msg) == "" {
		s.reply(id, toolResult("error: message is required"))
		return
	}
	if err := s.client.Send(msg); err != nil {
		s.reply(id, toolResult("send failed: "+err.Error()))
		return
	}
	s.reply(id, toolResult("sent."))
}

func (s *server) toolPoll(id json.RawMessage) {
	msgs, err := s.client.Poll()
	if err != nil {
		s.reply(id, toolResult("poll failed: "+err.Error()))
		return
	}
	s.reply(id, toolResult(renderMessages(msgs)))
}

func (s *server) toolWait(id json.RawMessage, args map[string]interface{}) {
	timeout := parseTimeout(args["timeout"])
	msgs, woken, err := s.client.Wait(timeout)
	if err != nil {
		s.reply(id, toolResult("wait failed: "+err.Error()))
		return
	}
	if !woken && len(msgs) == 0 {
		s.reply(id, toolResult("(timeout, no new messages)"))
		return
	}
	s.reply(id, toolResult(renderMessages(msgs)))
}

func (s *server) toolGo(id json.RawMessage, args map[string]interface{}) {
	reply, _ := args["reply"].(string)
	timeout := parseGoTimeout(args["timeout"])
	msgs, woken, err := s.client.Go(reply, timeout)
	if err != nil {
		s.reply(id, toolResult("go failed: "+err.Error()))
		return
	}
	var b strings.Builder
	if strings.TrimSpace(reply) != "" {
		b.WriteString("(reply sent)\n")
	}
	if !woken && len(msgs) == 0 {
		b.WriteString("(timeout, no mention)")
	} else {
		b.WriteString(renderMessages(msgs))
	}
	s.reply(id, toolResult(b.String()))
}

func (s *server) toolStatus(id json.RawMessage) {
	up, agents, err := s.client.Status()
	if err != nil {
		s.reply(id, toolResult("status failed: "+err.Error()))
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "writer daemon: %s\n", map[bool]string{true: "running", false: "down"}[up])
	fmt.Fprintf(&b, "this agent: %s\n", s.client.Agent)
	b.WriteString("roster:\n")
	for _, a := range agents {
		fmt.Fprintf(&b, "  - %-10s %-9s cursor=%d\n", a.Name, a.Liveness, a.Cursor)
	}
	s.reply(id, toolResult(b.String()))
}

func renderMessages(msgs []bus.Message) string {
	if len(msgs) == 0 {
		return "(no new messages)"
	}
	var b strings.Builder
	for _, m := range msgs {
		b.WriteString(m.String())
		b.WriteByte('\n')
	}
	return b.String()
}

// parseGoTimeout is like parseTimeout but also accepts the literal "sleep"
// (deep sleep: wait indefinitely, only woken by an @mention).
func parseGoTimeout(v interface{}) time.Duration {
	if s, ok := v.(string); ok && strings.EqualFold(strings.TrimSpace(s), "sleep") {
		return 0
	}
	return parseTimeout(v)
}
func parseTimeout(v interface{}) time.Duration {
	switch t := v.(type) {
	case float64:
		return time.Duration(t) * time.Second
	case string:
		t = strings.TrimSpace(strings.TrimSuffix(t, "s"))
		if n, err := strconv.ParseFloat(t, 64); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return 0 // wait forever
}
