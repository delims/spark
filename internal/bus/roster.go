package bus

import (
	"bufio"
	"os"
	"strings"
)

// Roster is the set of known agent display names, keyed by lowercase name.
type Roster struct {
	// lower -> display name
	members map[string]string
}

// DefaultRoster is used when no roster.conf exists yet.
var DefaultRoster = []string{"Zara", "Nora", "Cora"}

// LoadRoster reads roster.conf. Format: one "name" per line, or legacy
// "id:name". Lines starting with # and blank lines are ignored. "all" is a
// reserved broadcast target and is not treated as a member.
func LoadRoster(path string) (*Roster, error) {
	r := &Roster{members: map[string]string{}}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			for _, n := range DefaultRoster {
				r.members[strings.ToLower(n)] = n
			}
			return r, nil
		}
		return nil, err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Support legacy "id:name"; take the field after the last colon.
		name := line
		if i := strings.LastIndex(line, ":"); i >= 0 {
			name = strings.TrimSpace(line[i+1:])
		}
		if name == "" || strings.EqualFold(name, "all") {
			continue
		}
		r.members[strings.ToLower(name)] = name
	}
	return r, sc.Err()
}

// Has reports whether a lowercase name is a roster member.
func (r *Roster) Has(lower string) bool {
	_, ok := r.members[strings.ToLower(lower)]
	return ok
}

// Names returns all display names (unordered).
func (r *Roster) Names() []string {
	out := make([]string, 0, len(r.members))
	for _, v := range r.members {
		out = append(out, v)
	}
	return out
}

// mentionTrim strips surrounding punctuation commonly attached to a mention,
// e.g. "@Bob," or "@Bob." -> "Bob".
const mentionTrim = ",.;:!?)(\"'“”，。、！？；：）（"

// ParseMentions extracts @mentions from message text. It returns the set of
// lowercase targets to wake. "@all" expands to the special "all" target.
// A token must match a roster member (case-insensitive) to count, except
// "all" which is always honored. Mentions are delimited by whitespace and
// have trailing punctuation trimmed.
func ParseMentions(text string, roster *Roster) []string {
	seen := map[string]bool{}
	var out []string
	for _, field := range strings.Fields(text) {
		if !strings.HasPrefix(field, "@") || len(field) == 1 {
			continue
		}
		token := strings.TrimRight(field[1:], mentionTrim)
		if token == "" {
			continue
		}
		lower := strings.ToLower(token)
		if lower == "all" {
			if !seen["all"] {
				seen["all"] = true
				out = append(out, "all")
			}
			continue
		}
		if roster.Has(lower) && !seen[lower] {
			seen[lower] = true
			out = append(out, lower)
		}
	}
	return out
}
