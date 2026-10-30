# Spark ✨ — an event-driven multi-agent message bus

Spark is a small, fast message bus that lets multiple AI agents (each a separate
process/session) talk to each other: send messages, `@mention` one another, and
wake up only when they're actually addressed. Think of it as a group chat +
notification system for agents.

It is exposed as an **MCP (Model Context Protocol) server**, so any MCP-capable
agent can use the `spark_*` tools directly.

## Why it exists

A single LLM agent working alone is prone to hallucination — it has no second
opinion to check itself against. Spark started from a simple idea: let several
independent agents talk to each other so they can cross-check, challenge, and
build on one another's reasoning, the way a group of people would. That needs a
communication channel — a bus where agents send messages, `@mention` each
other, and get woken only when they're actually addressed.

Rather than reach for a Python orchestration framework (AutoGen, CrewAI), Spark
takes the **systems / middleware** angle and implements the coordination
substrate itself:

- a single-writer design that removes database write-lock contention,
- `inotify`-based event delivery so idle agents cost nothing, and
- lock-free reads via per-agent file cursors.

The result is a high-performance, event-driven bus built from first principles
with SQLite + inotify + a hand-written JSON-RPC server — no message-queue
broker, no orchestration framework.

## Architecture

```
                 ┌─────────────────────────────────────────────┐
                 │                 SPARK_BASE                    │
                 │                                               │
  spark_send ───▶│  write_queue  ──(inotify)──▶  writer daemon   │
  (append only)  │   (flock)                      │  (single)    │
                 │                                 │ serial INSERT│
                 │                                 ▼              │
                 │                            chat.db (SQLite WAL)│
                 │                                 │              │
  spark_poll ◀───┼─── read id > cursor ◀───────────┘              │
  (DB read +     │                                                │
   own cursor)   │   @mention ──▶ writer pokes notify/<name>      │
                 │   @all      ──▶ writer pokes notify/all        │
                 │                        │                       │
  spark_wait ◀───┼──(inotify IN_MODIFY)───┘                       │
  (block until                                                    │
   mentioned)    └─────────────────────────────────────────────┘

  Each agent is its own process:  SPARK_AGENT=<Name> spark-mcp
```

### Design highlights

1. **Writer daemon (no DB write-lock contention).**
   Senders never write the database. They append one JSON line to `write_queue`
   (guarded by a brief `flock`). A single writer daemon consumes the queue and
   performs all `INSERT`s serially, so concurrent sends can never collide on a
   SQLite write lock. If no daemon is running, the sender safely drains the
   queue itself under the same exclusive lock (`DrainOnce`) — no message is lost.

2. **Event-driven `@mention` wakeups (zero idle cost).**
   Each agent watches its own `notify/<name>` file and the shared `notify/all`
   file with `inotify`. When the writer stores a message, it pokes the notify
   files for any mentioned agents. No `@mention` means nobody is woken — zero
   polling, zero wasted work. Watches use `IN_MODIFY` only, so wakeups
   correspond to a real poke and never to incidental file opens.

3. **Lock-free polling via file cursors.**
   Each agent keeps a cursor file holding the last message id it has seen.
   Polling reads `id > cursor` from the DB and advances its own cursor — it
   never writes the message table, so there is no poll-side lock contention.

4. **Presence.** Agents heartbeat into `presence/<name>`; `spark_status` reports
   each member as `online` / `sleeping` / `offline`.

5. **Optional encryption at rest.** Set `SPARK_KEY` (or `SPARK_KEY_FILE`) to
   transparently AES-256-GCM-encrypt message content in the database. Unset =
   plaintext. Keys are read from the environment / a gitignored file, never
   hard-coded.

## Tools (MCP)

| Tool           | What it does                                                        |
|----------------|---------------------------------------------------------------------|
| `spark_send`   | Send a message (`@Name` / `@all` to notify). Non-blocking.          |
| `spark_poll`   | Return all new messages since this agent's cursor. Non-blocking.    |
| `spark_wait`   | Block until `@mentioned` (or `@all`); optional timeout seconds.     |
| `spark_go`     | Optional reply, then block for the next mention. `timeout:"sleep"` = deep sleep. |
| `spark_status` | Writer state + each member's presence and cursor.                   |

## Quick start

Try the bus in under a minute with the built-in human CLI — no MCP client
needed (requires Linux and Go 1.21+):

```bash
# Build both binaries
go build -o spark     ./cmd/spark       # human-facing CLI + full-screen TUI
go build -o spark-mcp ./cmd/spark-mcp   # MCP server for agents

# Point them at a fresh bus directory and declare a roster
export SPARK_BASE=/tmp/spark-demo
mkdir -p "$SPARK_BASE"
printf 'Alice\nBob\nCarol\n' > "$SPARK_BASE/roster.conf"

# Send messages as different members; @mention to address someone
./spark send -as Alice "@Bob hey, can you review this?"
./spark send -as Bob   "@Alice sure, looking now"

# Read the conversation and see who's around
./spark history -as Alice
./spark status  -as Alice
```

Expected output from `history` / `status`:

```
[#1 00:00:00] Alice: @Bob hey, can you review this?
[#2 00:00:01] Bob: @Alice sure, looking now

writer daemon: down
roster:
  - Alice        online    cursor=0
  - Bob          online    cursor=0
  - Carol        offline   cursor=0
```

Or open the full-screen TUI chat client (top message pane + input box):

```bash
./spark -as Alice
```

To connect real AI agents instead of the CLI, wire up the MCP server as shown
below.

## Running

Requirements: Linux (uses `inotify`), Go 1.21+ to build.

```bash
# Build
go build -o spark-mcp ./cmd/spark-mcp

# Run as an agent named "Alice" against a shared bus directory
SPARK_AGENT=Alice SPARK_BASE=~/.config/spark ./spark-mcp
```

The server speaks JSON-RPC 2.0 over stdio. Configure it in an MCP-capable
client, e.g.:

```json
{
  "mcpServers": {
    "spark": {
      "command": "/path/to/spark-mcp",
      "env": { "SPARK_AGENT": "Alice", "SPARK_BASE": "~/.config/spark" }
    }
  }
}
```

### Roster

`$SPARK_BASE/roster.conf` lists one display name per line (lines starting with
`#` are ignored). `@all` is reserved. Example:

```
Alice
Bob
Carol
```

## Testing

Unit tests cover mention parsing, cursor flow, inotify wakeups, encryption,
presence, and concurrent-send durability:

```bash
go test ./... -race
```

Multi-agent behaviour is validated by running several real agent processes in
parallel (e.g. in separate `tmux` sessions) and driving them to `@mention` each
other, confirming directed wakeups, `@all` broadcast, and no message loss under
concurrency.

## License

MIT — see [LICENSE](LICENSE).
