package main

import "testing"

func TestWrapInput(t *testing.T) {
	// Width 4; "abcdef" wraps to "abcd","ef".
	rows, cr, cc := wrapInput([]rune("abcdef"), 6, 4)
	if len(rows) != 2 || rows[0] != "abcd" || rows[1] != "ef" {
		t.Fatalf("wrap rows = %q", rows)
	}
	if cr != 1 || cc != 2 { // cursor at end: row 1, col 2
		t.Fatalf("cursor end = (%d,%d), want (1,2)", cr, cc)
	}

	// Cursor in the middle.
	_, cr, cc = wrapInput([]rune("abcdef"), 2, 4)
	if cr != 0 || cc != 2 {
		t.Fatalf("cursor mid = (%d,%d), want (0,2)", cr, cc)
	}

	// Wide chars: each CJK rune is width 2, so width 4 holds 2 per row.
	rows, _, _ = wrapInput([]rune("你好世界"), 4, 4)
	if len(rows) != 2 || rows[0] != "你好" || rows[1] != "世界" {
		t.Fatalf("wide wrap rows = %q", rows)
	}

	// Empty input yields a single empty row with cursor at origin.
	rows, cr, cc = wrapInput(nil, 0, 10)
	if len(rows) != 1 || rows[0] != "" || cr != 0 || cc != 0 {
		t.Fatalf("empty = rows %q cur (%d,%d)", rows, cr, cc)
	}
}

func TestWrapLinePreservingANSI(t *testing.T) {
	// Plain hard wrap at width 5.
	rows := wrapLinePreservingANSI("abcdefghij", 5)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d: %q", len(rows), rows)
	}
	// Each row's visible width must not exceed 5.
	for _, r := range rows {
		if visibleCells(r) > 5 {
			t.Fatalf("row exceeds width: %q (%d cells)", r, visibleCells(r))
		}
	}

	// Wide chars: width 4 holds two CJK runes per row.
	rows = wrapLinePreservingANSI("你好世界你好", 4)
	if len(rows) != 3 {
		t.Fatalf("CJK wrap want 3 rows, got %d: %q", len(rows), rows)
	}

	// ANSI styling must be preserved and not counted toward width.
	styled := ansiCyan + "hello world" + ansiReset
	rows = wrapLinePreservingANSI(styled, 5)
	if len(rows) < 2 {
		t.Fatalf("styled should wrap, got %q", rows)
	}
	// Continuation rows should re-emit the active color.
	if !containsSeq(rows[1], ansiCyan) {
		t.Fatalf("continuation row lost active style: %q", rows[1])
	}
}

// visibleCells counts display width ignoring ANSI escape sequences.
func visibleCells(s string) int {
	w := 0
	i := 0
	for i < len(s) {
		if s[i] == 0x1b { // skip the whole escape sequence
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
			i = j
			continue
		}
		r, size := decodeRune(s[i:])
		w += runeWidth(r)
		i += size
	}
	return w
}

func decodeRune(s string) (rune, int) {
	for i, r := range s {
		_ = i
		return r, len(string(r))
	}
	return 0, 1
}

func containsSeq(s, seq string) bool {
	return len(s) >= len(seq) && (indexOf(s, seq) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
