package bus

import (
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Message is a single bus message.
type Message struct {
	ID      int64  `json:"id"`
	TS      int64  `json:"ts"` // unix millis
	Sender  string `json:"sender"`
	Content string `json:"content"`
}

// Store wraps the SQLite message database.
type Store struct {
	db     *sql.DB
	cipher *Cipher
}

// OpenStore opens (and migrates) the SQLite DB in WAL mode. The pragmas make
// reads and writes concurrency-friendly: the single writer daemon serializes
// INSERTs while pollers only ever read. If a SPARK_KEY is configured, message
// content is transparently encrypted at rest.
func OpenStore(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS messages (
		id      INTEGER PRIMARY KEY AUTOINCREMENT,
		ts      INTEGER NOT NULL,
		sender  TEXT    NOT NULL,
		content TEXT    NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	ciph, err := LoadCipher()
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db: db, cipher: ciph}, nil
}

// Close closes the underlying DB.
func (s *Store) Close() error { return s.db.Close() }

// Insert appends a message and returns its assigned id. Only the writer daemon
// (or the send fallback path) calls this. Content is encrypted at rest when a
// key is configured.
func (s *Store) Insert(sender, content string) (int64, error) {
	stored, err := s.cipher.Encrypt(content)
	if err != nil {
		return 0, err
	}
	ts := time.Now().UnixMilli()
	res, err := s.db.Exec(`INSERT INTO messages (ts, sender, content) VALUES (?, ?, ?)`, ts, sender, stored)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// Since returns all messages with id greater than afterID, ordered by id.
// This is a pure read — pollers never write to the message table. Content is
// decrypted on the way out.
func (s *Store) Since(afterID int64) ([]Message, error) {
	rows, err := s.db.Query(`SELECT id, ts, sender, content FROM messages WHERE id > ? ORDER BY id ASC`, afterID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var m Message
		var stored string
		if err := rows.Scan(&m.ID, &m.TS, &m.Sender, &stored); err != nil {
			return nil, err
		}
		m.Content, err = s.cipher.Decrypt(stored)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// MaxID returns the current highest message id (0 if empty).
func (s *Store) MaxID() (int64, error) {
	var id sql.NullInt64
	if err := s.db.QueryRow(`SELECT MAX(id) FROM messages`).Scan(&id); err != nil {
		return 0, err
	}
	if id.Valid {
		return id.Int64, nil
	}
	return 0, nil
}

// Recent returns up to the last n messages ordered by id ascending. Content is
// decrypted on the way out.
func (s *Store) Recent(n int) ([]Message, error) {
	if n <= 0 {
		n = 20
	}
	rows, err := s.db.Query(
		`SELECT id, ts, sender, content FROM (
			SELECT id, ts, sender, content FROM messages ORDER BY id DESC LIMIT ?
		) ORDER BY id ASC`, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var m Message
		var stored string
		if err := rows.Scan(&m.ID, &m.TS, &m.Sender, &stored); err != nil {
			return nil, err
		}
		m.Content, err = s.cipher.Decrypt(stored)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
