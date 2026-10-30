package bus

import (
	"os"
	"strconv"
	"strings"
)

// ReadCursor returns the last-seen message id for an agent. A missing cursor
// means the agent has seen nothing yet (0). Reading a cursor never touches the
// message DB — it only reads this per-agent file.
func (l *Layout) ReadCursor(agent string) (int64, error) {
	b, err := os.ReadFile(l.CursorFile(agent))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, nil
	}
	return strconv.ParseInt(s, 10, 64)
}

// WriteCursor atomically persists the agent's cursor by writing to a temp file
// and renaming it into place.
func (l *Layout) WriteCursor(agent string, id int64) error {
	path := l.CursorFile(agent)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(id, 10)), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
