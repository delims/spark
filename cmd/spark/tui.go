package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/delims/spark/internal/bus"
)

// cmdTUI launches the full-screen chat UI: one command puts the user "online",
// showing a live message pane on top and an input box at the bottom — like a
// terminal group chat. New messages stream in via inotify; typed lines are
// sent to the bus.
//
// Keys:
//
//	Enter          send the current line
//	Backspace      delete the char before the cursor
//	Left / Right   move the input cursor
//	Home / End     jump to start / end of input
//	Up / Down      scroll the message pane
//	PgUp / PgDn    scroll a page
//	Ctrl-C         quit (Esc alone also quits)
func cmdTUI(args []string) {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	as := fs.String("as", "", "your display name")
	_ = fs.Parse(args)

	c := newClient(*as)

	t, err := enterRaw()
	if err != nil {
		fmt.Fprintln(os.Stderr, "spark tui:", err)
		os.Exit(1)
	}
	defer t.close()

	ui := &chatUI{client: c, term: t}
	ui.run()
}

type chatUI struct {
	client *bus.Client
	term   *term

	renderReq chan struct{}

	mu     sync.Mutex
	lines  []string // message lines (ring-bounded)
	input  []rune   // input buffer
	curpos int      // cursor index within input (0..len)
	scroll int      // lines scrolled up from the bottom (0 = following)
}

const maxLines = 1000

// ANSI styles.
const (
	ansiReset = "\x1b[0m"
	ansiDim   = "\x1b[2m"
	ansiCyan  = "\x1b[36m"
	ansiBold  = "\x1b[1m"
	ansiRevse = "\x1b[7m"
)

func (u *chatUI) run() {
	if hist, err := u.client.History(80); err == nil {
		for _, m := range hist {
			u.lines = append(u.lines, formatMessage(m))
		}
	}

	u.renderReq = make(chan struct{}, 1)
	stop := make(chan struct{})

	// Coalesced render loop: many triggers (keystrokes, new messages, a flood
	// of resize signals during a window drag) collapse into at most one repaint
	// per debounce window, so the screen stays clean instead of racing.
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-u.renderReq:
				u.render()
				time.Sleep(16 * time.Millisecond) // ~60fps cap; absorbs bursts
			}
		}
	}()
	u.requestRender()

	go func() {
		_ = u.client.Watch(stop, func(m bus.Message) {
			u.mu.Lock()
			u.push(formatMessage(m))
			u.mu.Unlock()
			u.requestRender()
		})
	}()

	// Re-render on terminal resize (SIGWINCH). Signals are coalesced by the
	// render loop above, so a drag that emits a burst repaints smoothly.
	winch := make(chan os.Signal, 1)
	signal.Notify(winch, syscall.SIGWINCH)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-winch:
				u.requestRender()
			}
		}
	}()

	u.readLoop()
	close(stop)
	// Restore the terminal and exit promptly; background watch goroutines may
	// be parked in a blocking poll, so don't rely on unwinding through them.
	u.term.close()
	os.Exit(0)
}

// requestRender signals the render loop to repaint. It never blocks: if a
// repaint is already pending, the trigger is dropped (coalesced).
func (u *chatUI) requestRender() {
	select {
	case u.renderReq <- struct{}{}:
	default:
	}
}

// readLoop reads raw bytes from the terminal fd, decoding UTF-8 runes and
// escape sequences for special keys. All reads go through the raw fd (not
// os.Stdin) so they don't contend with Go's network poller on the same fd.
func (u *chatUI) readLoop() {
	buf := make([]byte, 0, 32)
	tmp := make([]byte, 64)
	for {
		n := u.term.readBlocking(tmp)
		if n <= 0 {
			return
		}
		buf = append(buf, tmp[:n]...)
		for len(buf) > 0 {
			// If the buffer is exactly a lone ESC, it may be the start of an
			// escape sequence whose tail hasn't arrived yet. Peek briefly.
			if len(buf) == 1 && buf[0] == 27 {
				var peek [32]byte
				if m := u.term.readTimeout(peek[:], 20*time.Millisecond); m > 0 {
					buf = append(buf, peek[:m]...)
				} else {
					return // genuine lone Esc → quit
				}
			}
			consumed, quit := u.handleInput(buf)
			if quit {
				return
			}
			if consumed == 0 {
				break // need more bytes to complete a sequence/rune
			}
			buf = buf[consumed:]
		}
	}
}

// handleInput processes the next key at the front of b. It returns how many
// bytes it consumed (0 if it needs more) and whether to quit.
func (u *chatUI) handleInput(b []byte) (consumed int, quit bool) {
	switch b[0] {
	case 3: // Ctrl-C
		return 1, true
	case '\r': // Enter → send
		u.submit()
		return 1, false
	case '\n': // Ctrl-J (LF) → insert a newline into the input
		u.insert('\n')
		return 1, false
	case 127, 8: // Backspace
		u.backspace()
		return 1, false
	case 27: // ESC — readLoop already peeked; a lone ESC here means quit.
		if len(b) == 1 {
			return 1, true
		}
		// Alt+Enter arrives as ESC + CR → insert a newline.
		if b[1] == '\r' || b[1] == '\n' {
			u.insert('\n')
			return 2, false
		}
		if b[1] == '[' || b[1] == 'O' {
			if adv, ok := u.handleCSI(b[2:]); ok {
				return 2 + adv, false
			}
			return 0, false // incomplete sequence; wait for more
		}
		// ESC + other: ignore just the ESC.
		return 1, false
	default:
		r, size := utf8.DecodeRune(b)
		if r == utf8.RuneError && size <= 1 {
			if !utf8.FullRune(b) {
				return 0, false // partial multibyte rune; wait
			}
			return 1, false // skip invalid byte
		}
		if r >= 32 {
			u.insert(r)
		}
		return size, false
	}
}

// handleCSI interprets the body of an ESC [ / ESC O sequence. Returns how many
// bytes after the introducer it consumed and whether it recognized a key.
func (u *chatUI) handleCSI(rest []byte) (adv int, ok bool) {
	if len(rest) == 0 {
		return 0, false
	}
	switch rest[0] {
	case 'A': // Up
		u.scrollBy(1)
		return 1, true
	case 'B': // Down
		u.scrollBy(-1)
		return 1, true
	case 'C': // Right
		u.moveCursor(1)
		return 1, true
	case 'D': // Left
		u.moveCursor(-1)
		return 1, true
	case 'H': // Home
		u.setCursor(0)
		return 1, true
	case 'F': // End
		u.setCursor(len(u.input))
		return 1, true
	case '5': // PgUp: ESC [ 5 ~
		if len(rest) >= 2 && rest[1] == '~' {
			u.scrollBy(u.pageSize())
			return 2, true
		}
		return 0, false
	case '6': // PgDn: ESC [ 6 ~
		if len(rest) >= 2 && rest[1] == '~' {
			u.scrollBy(-u.pageSize())
			return 2, true
		}
		return 0, false
	case '1', '7': // Home variants: ESC [ 1 ~ / ESC [ 7 ~
		if len(rest) >= 2 && rest[1] == '~' {
			u.setCursor(0)
			return 2, true
		}
		return 0, false
	case '4', '8': // End variants: ESC [ 4 ~ / ESC [ 8 ~
		if len(rest) >= 2 && rest[1] == '~' {
			u.setCursor(len(u.input))
			return 2, true
		}
		return 0, false
	case '3': // Delete: ESC [ 3 ~
		if len(rest) >= 2 && rest[1] == '~' {
			u.deleteAtCursor()
			return 2, true
		}
		return 0, false
	}
	return 1, true // unknown single-letter CSI: swallow it
}

// ---- editing operations ----

func (u *chatUI) insert(r rune) {
	u.mu.Lock()
	u.input = append(u.input, 0)
	copy(u.input[u.curpos+1:], u.input[u.curpos:])
	u.input[u.curpos] = r
	u.curpos++
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) backspace() {
	u.mu.Lock()
	if u.curpos > 0 {
		u.input = append(u.input[:u.curpos-1], u.input[u.curpos:]...)
		u.curpos--
	}
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) deleteAtCursor() {
	u.mu.Lock()
	if u.curpos < len(u.input) {
		u.input = append(u.input[:u.curpos], u.input[u.curpos+1:]...)
	}
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) moveCursor(d int) {
	u.mu.Lock()
	u.curpos += d
	if u.curpos < 0 {
		u.curpos = 0
	}
	if u.curpos > len(u.input) {
		u.curpos = len(u.input)
	}
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) setCursor(p int) {
	u.mu.Lock()
	u.curpos = p
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) scrollBy(d int) {
	u.mu.Lock()
	u.scroll += d
	if u.scroll < 0 {
		u.scroll = 0
	}
	// Clamp against the total number of visual (wrapped) rows at current width,
	// so scrolling matches what the pane actually renders.
	_, cols := u.term.size()
	total := 0
	for _, l := range u.lines {
		total += len(wrapLinePreservingANSI(l, cols))
	}
	if max := total - 1; max >= 0 && u.scroll > max {
		u.scroll = max
	}
	if u.scroll < 0 {
		u.scroll = 0
	}
	u.mu.Unlock()
	u.requestRender()
}

func (u *chatUI) pageSize() int {
	rows, _ := u.term.size()
	p := rows - 3
	if p < 1 {
		p = 1
	}
	return p
}

func (u *chatUI) submit() {
	u.mu.Lock()
	msg := strings.TrimSpace(string(u.input))
	u.input = u.input[:0]
	u.curpos = 0
	u.scroll = 0 // jump back to live tail after sending
	u.mu.Unlock()
	if msg == "" {
		u.requestRender()
		return
	}
	if err := u.client.Send(msg); err != nil {
		u.mu.Lock()
		u.push("[send error] " + err.Error())
		u.mu.Unlock()
	}
	u.requestRender()
}

func (u *chatUI) push(s string) {
	u.lines = append(u.lines, s)
	if len(u.lines) > maxLines {
		u.lines = u.lines[len(u.lines)-maxLines:]
	}
}

// formatMessage renders a message with a dim timestamp and a colored sender.
func formatMessage(m bus.Message) string {
	full := m.String() // "[#id HH:MM:SS] Sender: content"
	// Split into "[...]" prefix, "Sender:", and the rest for light coloring.
	close := strings.Index(full, "] ")
	if close < 0 {
		return full
	}
	meta := full[:close+1]
	rest := full[close+2:]
	colon := strings.Index(rest, ": ")
	if colon < 0 {
		return ansiDim + meta + ansiReset + " " + rest
	}
	sender := rest[:colon]
	body := rest[colon+2:]
	return ansiDim + meta + ansiReset + " " +
		ansiCyan + ansiBold + sender + ansiReset + ": " + body
}

// render repaints the whole screen: a header bar, the message pane (top-aligned
// like a chat log), a divider, and the input line.
func (u *chatUI) render() {
	rows, cols := u.term.size()
	if rows < 4 {
		rows = 4
	}
	if cols < 10 {
		cols = 10
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// Wrap the input across visual rows (honoring Ctrl-J newlines). The input
	// box has a fixed minimum height so it always looks like a multi-line
	// composer, and grows up to a cap, scrolling to keep the cursor in view.
	const prompt = "› "
	const minInputRows = 3
	const maxInputRows = 6
	promptW := stringWidth(prompt)
	inputRows, curRow, curCol := wrapInput(u.input, u.curpos, cols-promptW)
	if len(inputRows) > maxInputRows {
		first := curRow - (maxInputRows - 1)
		if first < 0 {
			first = 0
		}
		if first > len(inputRows)-maxInputRows {
			first = len(inputRows) - maxInputRows
		}
		inputRows = inputRows[first : first+maxInputRows]
		curRow -= first
	}
	inputH := len(inputRows)
	if inputH < minInputRows {
		inputH = minInputRows
	}

	// Row budget: 1 header + bodyRows messages + 1 divider + inputH input rows.
	bodyRows := rows - 2 - inputH
	if bodyRows < 1 {
		bodyRows = 1
		// On a very short terminal, shrink the input box back toward 1 line.
		inputH = rows - 2 - bodyRows
		if inputH < 1 {
			inputH = 1
		}
	}

	var b strings.Builder
	b.WriteString("\x1b[?25l\x1b[2J") // hide cursor, clear

	writeRow := func(row int, s string) {
		fmt.Fprintf(&b, "\x1b[%d;1H\x1b[2K", row)
		b.WriteString(s)
	}

	// Header bar (row 1): left = identity, right = key hints (shown only when
	// there's room). The hint lives here — never in the message stream.
	left := fmt.Sprintf(" spark ✨  you: %s ", u.client.Agent)
	if u.scroll > 0 {
		left += fmt.Sprintf("[↑%d ↓/Enter=live] ", u.scroll)
	}
	right := " Enter=send · ↑/↓=scroll · Ctrl-C=quit "
	lw := stringWidth(left)
	rw := stringWidth(right)
	var headerLine string
	if lw+rw <= cols {
		gap := cols - lw - rw
		headerLine = left + strings.Repeat(" ", gap) + right
	} else {
		// Not enough room for the hint; show just the identity, padded.
		hclip, hcw := clipWidth(left, cols)
		pad := cols - hcw
		if pad < 0 {
			pad = 0
		}
		headerLine = hclip + strings.Repeat(" ", pad)
	}
	writeRow(1, ansiRevse+headerLine+ansiReset)

	// Message pane (rows 2 .. 1+bodyRows). Each logical message is wrapped to
	// the current width into one or more visual rows, so resizing reflows the
	// text instead of truncating it. The scroll offset counts visual rows up
	// from the newest.
	var vis []string
	for _, logical := range u.lines {
		vis = append(vis, wrapLinePreservingANSI(logical, cols)...)
	}
	end := len(vis) - u.scroll
	if end < 0 {
		end = 0
	}
	if end > len(vis) {
		end = len(vis)
	}
	start := end - bodyRows
	if start < 0 {
		start = 0
	}
	visible := vis[start:end]
	for i := 0; i < bodyRows; i++ {
		s := ""
		if i < len(visible) {
			s = visible[i]
		}
		writeRow(2+i, s)
	}

	// Divider.
	dividerRow := 2 + bodyRows
	writeRow(dividerRow, ansiDim+strings.Repeat("─", cols)+ansiReset)

	// Input box: inputH rows below the divider. The prompt sits on the first;
	// blank rows pad the box out to its fixed height.
	inputTop := dividerRow + 1
	for i := 0; i < inputH; i++ {
		pfx := strings.Repeat(" ", promptW)
		if i == 0 {
			pfx = ansiBold + prompt + ansiReset
		}
		line := ""
		if i < len(inputRows) {
			line = inputRows[i]
		}
		writeRow(inputTop+i, pfx+line)
	}

	// Place the hardware cursor within the wrapped input.
	curScreenRow := inputTop + curRow
	curScreenCol := promptW + curCol + 1
	if curScreenCol > cols {
		curScreenCol = cols
	}
	fmt.Fprintf(&b, "\x1b[%d;%dH\x1b[?25h", curScreenRow, curScreenCol)

	os.Stdout.WriteString(b.String())
}

// wrapInput wraps the input runes into visual rows no wider than width display
// cells, breaking also on explicit '\n' (Ctrl-J) line breaks, and reports the
// (row, col) the cursor at index curpos lands on.
func wrapInput(input []rune, curpos, width int) (rows []string, curRow, curCol int) {
	if width < 1 {
		width = 1
	}
	var cur strings.Builder
	w := 0
	curRow, curCol = 0, 0
	for i, r := range input {
		if i == curpos {
			curRow = len(rows)
			curCol = w
		}
		if r == '\n' { // explicit newline: hard break, no glyph
			rows = append(rows, cur.String())
			cur.Reset()
			w = 0
			if i == curpos {
				curRow = len(rows)
				curCol = 0
			}
			continue
		}
		rw := runeWidth(r)
		if w+rw > width {
			rows = append(rows, cur.String())
			cur.Reset()
			w = 0
			if i == curpos {
				curRow = len(rows)
				curCol = 0
			}
		}
		cur.WriteRune(r)
		w += rw
	}
	// Cursor at the very end.
	if curpos >= len(input) {
		curRow = len(rows)
		curCol = w
	}
	rows = append(rows, cur.String())
	return rows, curRow, curCol
}

// wrapLinePreservingANSI splits a styled logical line into visual rows, each no
// wider than cols display cells. ANSI escape sequences are copied verbatim and
// do not count toward the width; the active style sequences seen so far are
// re-emitted at the start of each continuation row so colors/dim survive the
// break, and every row ends with a reset. Wrapping is by display cell (a hard
// wrap), which is correct for CJK and mixed-width text.
func wrapLinePreservingANSI(s string, cols int) []string {
	if cols < 1 {
		cols = 1
	}
	var rows []string
	var cur strings.Builder
	var active strings.Builder // style sequences active on the current row
	w := 0
	i := 0
	flush := func() {
		cur.WriteString(ansiReset)
		rows = append(rows, cur.String())
		cur.Reset()
		w = 0
		// Seed the next row with the styles active so far.
		cur.WriteString(active.String())
	}
	for i < len(s) {
		if s[i] == 0x1b { // escape sequence: copy verbatim, track as active
			j := i + 1
			if j < len(s) && s[j] == '[' {
				j++
				for j < len(s) && !(s[j] >= '@' && s[j] <= '~') {
					j++
				}
				if j < len(s) {
					j++
				}
			}
			seq := s[i:j]
			cur.WriteString(seq)
			if seq == ansiReset {
				active.Reset()
			} else {
				active.WriteString(seq)
			}
			i = j
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == '\n' { // explicit newline in the message → hard break
			flush()
			i += size
			continue
		}
		rw := runeWidth(r)
		if w+rw > cols {
			flush()
		}
		cur.WriteString(s[i : i+size])
		w += rw
		i += size
	}
	cur.WriteString(ansiReset)
	rows = append(rows, cur.String())
	return rows
}
