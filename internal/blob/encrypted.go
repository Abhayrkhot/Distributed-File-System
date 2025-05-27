package blob

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
)

// Encrypted wraps a Store with client-side AES-256-GCM, so the S3 tier only
// ever holds ciphertext. This replaces the prototype's AES-CTR stream cipher:
// GCM authenticates each object, so a tampered or truncated object fails to
// decrypt instead of returning garbage. The object key is bound as
// additional data, which stops an object being swapped under another key.
//
// Object layout: nonce (12 bytes) || ciphertext+tag.
type Encrypted struct {
	inner Store
	aead  cipher.AEAD
}

var ErrDecrypt = errors.New("blob: decryption failed")

// NewEncrypted needs a 32-byte key.
func NewEncrypted(inner Store, key []byte) (*Encrypted, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("blob: encryption key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Encrypted{inner: inner, aead: aead}, nil
}

func (e *Encrypted) Put(ctx context.Context, key string, data []byte) error {
	ns := e.aead.NonceSize()
	out := make([]byte, ns, ns+len(data)+e.aead.Overhead())
	if _, err := io.ReadFull(rand.Reader, out); err != nil {
		return err
	}
	out = e.aead.Seal(out, out[:ns], data, []byte(key))
	return e.inner.Put(ctx, key, out)
}

func (e *Encrypted) Get(ctx context.Context, key string) ([]byte, error) {
	b, err := e.inner.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	ns := e.aead.NonceSize()
	if len(b) < ns+e.aead.Overhead() {
		return nil, ErrDecrypt
	}
	pt, err := e.aead.Open(nil, b[:ns], b[ns:], []byte(key))
	if err != nil {
		return nil, ErrDecrypt
	}
	return pt, nil
}

func (e *Encrypted) Exists(ctx context.Context, key string) (bool, error) {
	return e.inner.Exists(ctx, key)
}

func (e *Encrypted) List(ctx context.Context, prefix string, fn func(string) error) error {
	return e.inner.List(ctx, prefix, fn)
}

func (e *Encrypted) Delete(ctx context.Context, key string) error {
	return e.inner.Delete(ctx, key)
}
