package main

// runeWidth returns the number of terminal cells a rune occupies: 0 for most
// combining marks, 2 for East-Asian wide and emoji ranges, 1 otherwise. This
// is a compact approximation (not a full Unicode width table) that is accurate
// for the CJK and emoji a chat UI actually encounters.
func runeWidth(r rune) int {
	if r == 0 {
		return 0
	}
	// Zero-width: combining marks, ZWJ, variation selectors.
	if (r >= 0x0300 && r <= 0x036F) ||
		(r >= 0x200B && r <= 0x200F) ||
		(r >= 0xFE00 && r <= 0xFE0F) ||
		r == 0x200D {
		return 0
	}
	// Wide ranges (CJK, Hangul, Kana, fullwidth forms, common emoji).
	switch {
	case r >= 0x1100 && r <= 0x115F, // Hangul Jamo
		r >= 0x2E80 && r <= 0x303E, // CJK radicals, Kangxi
		r >= 0x3041 && r <= 0x33FF, // Hiragana..CJK symbols
		r >= 0x3400 && r <= 0x4DBF, // CJK Ext A
		r >= 0x4E00 && r <= 0x9FFF, // CJK Unified
		r >= 0xA000 && r <= 0xA4CF, // Yi
		r >= 0xAC00 && r <= 0xD7A3, // Hangul syllables
		r >= 0xF900 && r <= 0xFAFF, // CJK compat
		r >= 0xFE30 && r <= 0xFE4F, // CJK compat forms
		r >= 0xFF00 && r <= 0xFF60, // Fullwidth forms
		r >= 0xFFE0 && r <= 0xFFE6,
		r >= 0x1F000 && r <= 0x1FAFF, // emoji & symbols
		r >= 0x20000 && r <= 0x3FFFD: // CJK Ext B+
		return 2
	}
	return 1
}

// stringWidth returns the total display width of s.
func stringWidth(s string) int {
	w := 0
	for _, r := range s {
		w += runeWidth(r)
	}
	return w
}

// clipWidth truncates s so its display width does not exceed max cells,
// returning the clipped string and its actual width.
func clipWidth(s string, max int) (string, int) {
	if max <= 0 {
		return "", 0
	}
	w := 0
	var b []rune
	for _, r := range s {
		rw := runeWidth(r)
		if w+rw > max {
			break
		}
		b = append(b, r)
		w += rw
	}
	return string(b), w
}
