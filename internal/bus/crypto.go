package bus

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"os"
	"strings"
)

// cipherPrefix marks a stored value as encrypted so mixed plaintext/ciphertext
// databases (e.g. after enabling a key later) stay readable.
const cipherPrefix = "enc:v1:"

// Cipher provides optional symmetric encryption of message content. A nil
// Cipher means encryption is disabled (plaintext storage).
type Cipher struct {
	aead cipher.AEAD
}

// LoadCipher builds a Cipher from SPARK_KEY (a passphrase or base64 key) or
// SPARK_KEY_FILE (path to a file whose contents are the key). It returns
// (nil, nil) when no key is configured, meaning encryption is off.
//
// The provided key material is hashed with SHA-256 to derive a 32-byte
// AES-256 key, so any-length passphrases are accepted.
func LoadCipher() (*Cipher, error) {
	raw := strings.TrimSpace(os.Getenv("SPARK_KEY"))
	if raw == "" {
		if path := strings.TrimSpace(os.Getenv("SPARK_KEY_FILE")); path != "" {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			raw = strings.TrimSpace(string(b))
		}
	}
	if raw == "" {
		return nil, nil // encryption disabled
	}
	// Accept base64 keys directly; otherwise treat as a passphrase.
	var keyMaterial []byte
	if decoded, err := base64.StdEncoding.DecodeString(raw); err == nil && len(decoded) >= 16 {
		keyMaterial = decoded
	} else {
		keyMaterial = []byte(raw)
	}
	sum := sha256.Sum256(keyMaterial)
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Cipher{aead: aead}, nil
}

// Encrypt returns a prefixed base64 ciphertext for the given plaintext. A nil
// Cipher returns the plaintext unchanged.
func (c *Cipher) Encrypt(plaintext string) (string, error) {
	if c == nil {
		return plaintext, nil
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	sealed := c.aead.Seal(nonce, nonce, []byte(plaintext), nil)
	return cipherPrefix + base64.StdEncoding.EncodeToString(sealed), nil
}

// Decrypt reverses Encrypt. Values without the cipher prefix are returned as-is
// (plaintext), so databases that mix encrypted and legacy plaintext rows still
// read correctly. A nil Cipher only passes through unprefixed values.
func (c *Cipher) Decrypt(stored string) (string, error) {
	if !strings.HasPrefix(stored, cipherPrefix) {
		return stored, nil // plaintext
	}
	if c == nil {
		return "", errors.New("message is encrypted but no SPARK_KEY is set")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(stored, cipherPrefix))
	if err != nil {
		return "", err
	}
	ns := c.aead.NonceSize()
	if len(raw) < ns {
		return "", errors.New("ciphertext too short")
	}
	nonce, ct := raw[:ns], raw[ns:]
	pt, err := c.aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
