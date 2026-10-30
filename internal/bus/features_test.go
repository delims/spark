package bus

import (
	"os"
	"strconv"
	"testing"
	"time"
)

func TestCipherRoundTrip(t *testing.T) {
	t.Setenv("SPARK_KEY", "a-test-passphrase-123")
	c, err := LoadCipher()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected a cipher with SPARK_KEY set")
	}
	plain := "@Nora secret: the launch code is 42 🌸"
	enc, err := c.Encrypt(plain)
	if err != nil {
		t.Fatal(err)
	}
	if enc == plain {
		t.Fatal("ciphertext equals plaintext")
	}
	dec, err := c.Decrypt(enc)
	if err != nil {
		t.Fatal(err)
	}
	if dec != plain {
		t.Fatalf("round-trip mismatch: %q != %q", dec, plain)
	}
}

func TestCipherDisabledPassthrough(t *testing.T) {
	os.Unsetenv("SPARK_KEY")
	os.Unsetenv("SPARK_KEY_FILE")
	c, err := LoadCipher()
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatal("expected nil cipher when no key configured")
	}
	// nil cipher passes plaintext through unchanged.
	got, _ := c.Encrypt("hello")
	if got != "hello" {
		t.Fatalf("nil Encrypt changed plaintext: %q", got)
	}
	got, _ = c.Decrypt("hello")
	if got != "hello" {
		t.Fatalf("nil Decrypt changed plaintext: %q", got)
	}
}

func TestStoreEncryptedAtRest(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("SPARK_KEY", "store-key")
	l, _ := NewLayout(dir)
	_ = l.EnsureDirs()
	st, err := OpenStore(l.DB)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	secret := "top secret content"
	if _, err := st.Insert("Zara", secret); err != nil {
		t.Fatal(err)
	}
	// Read back through the API: decrypted.
	msgs, _ := st.Since(0)
	if len(msgs) != 1 || msgs[0].Content != secret {
		t.Fatalf("decrypted read failed: %+v", msgs)
	}
	// Read the raw column: must NOT contain the plaintext.
	var raw string
	if err := st.db.QueryRow(`SELECT content FROM messages WHERE id=1`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw == secret {
		t.Fatal("content stored in plaintext despite key")
	}
}

func TestPresenceLiveness(t *testing.T) {
	dir := t.TempDir()
	l, _ := NewLayout(dir)
	_ = l.EnsureDirs()

	if got := l.LivenessOf("ghost"); got != Offline {
		t.Fatalf("unknown agent should be offline, got %s", got)
	}
	if err := l.Heartbeat("zara"); err != nil {
		t.Fatal(err)
	}
	if got := l.LivenessOf("zara"); got != Online {
		t.Fatalf("fresh heartbeat should be online, got %s", got)
	}
	// Simulate an old heartbeat by writing a stale timestamp.
	stale := time.Now().Add(-10 * time.Minute).UnixMilli()
	_ = os.WriteFile(l.presenceFile("nora"), []byte(strconv.FormatInt(stale, 10)), 0o644)
	if got := l.LivenessOf("nora"); got != Offline {
		t.Fatalf("10-min-old heartbeat should be offline, got %s", got)
	}
}

func TestGoLoopReplyThenWait(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(dir+"/roster.conf", []byte("Zara\nNora\nCora\n"), 0o644)
	zara, _ := NewClient("Zara", dir)
	nora, _ := NewClient("Nora", dir)

	// Zara's Go sends a reply mentioning Nora, then would wait; run it in a
	// goroutine with a short timeout so the test doesn't block.
	done := make(chan struct{})
	go func() {
		_, _, _ = zara.Go("@Nora ping from go", 1*time.Second)
		close(done)
	}()
	<-done

	// Nora should see the reply Zara sent.
	msgs, woken, err := nora.Wait(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !woken || len(msgs) == 0 {
		t.Fatalf("Nora should have been woken by Zara's go-reply, woken=%v msgs=%d", woken, len(msgs))
	}
	if msgs[len(msgs)-1].Content != "@Nora ping from go" {
		t.Fatalf("unexpected content: %q", msgs[len(msgs)-1].Content)
	}
}
