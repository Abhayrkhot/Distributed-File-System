package blob

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// conformance runs the same checks against every Store implementation.
func conformance(t *testing.T, s Store) {
	ctx := context.Background()
	prefix := fmt.Sprintf("test-%d/", time.Now().UnixNano())
	k1, k2 := prefix+"a/1", prefix+"b/2"

	if ok, err := s.Exists(ctx, k1); err != nil || ok {
		t.Fatalf("exists before put: %v %v", ok, err)
	}
	if _, err := s.Get(ctx, k1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get missing: want ErrNotFound, got %v", err)
	}
	payload := make([]byte, 1<<20)
	rand.Read(payload)
	if err := s.Put(ctx, k1, payload); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, k2, []byte("two")); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, k1)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("get: %v", err)
	}
	if ok, _ := s.Exists(ctx, k1); !ok {
		t.Fatal("exists after put")
	}
	var keys []string
	if err := s.List(ctx, prefix, func(k string) error { keys = append(keys, k); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] != k1 || keys[1] != k2 {
		t.Fatalf("list: %v", keys)
	}
	for _, k := range keys {
		if err := s.Delete(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	if ok, _ := s.Exists(ctx, k1); ok {
		t.Fatal("exists after delete")
	}
}

func TestFSConformance(t *testing.T) {
	s, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, s)
}

func TestEncryptedConformance(t *testing.T) {
	inner, _ := NewFS(t.TempDir())
	key := make([]byte, 32)
	rand.Read(key)
	e, err := NewEncrypted(inner, key)
	if err != nil {
		t.Fatal(err)
	}
	conformance(t, e)
}

func TestEncryptedIsCiphertextAndAuthenticated(t *testing.T) {
	ctx := context.Background()
	inner, _ := NewFS(t.TempDir())
	key := make([]byte, 32)
	rand.Read(key)
	e, _ := NewEncrypted(inner, key)
	plain := []byte("the quick brown fox jumps over the lazy dog")
	if err := e.Put(ctx, "k", plain); err != nil {
		t.Fatal(err)
	}
	raw, _ := inner.Get(ctx, "k")
	if bytes.Contains(raw, plain) {
		t.Fatal("plaintext visible in backing store")
	}
	// Flip one ciphertext bit: GCM must reject it.
	raw[len(raw)-1] ^= 1
	inner.Put(ctx, "k", raw)
	if _, err := e.Get(ctx, "k"); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("tampered object: want ErrDecrypt, got %v", err)
	}
	// Moving a valid object to another key must also fail (key is AAD).
	e.Put(ctx, "a", plain)
	moved, _ := inner.Get(ctx, "a")
	inner.Put(ctx, "b", moved)
	if _, err := e.Get(ctx, "b"); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("swapped object: want ErrDecrypt, got %v", err)
	}
	other := make([]byte, 32)
	rand.Read(other)
	e2, _ := NewEncrypted(inner, other)
	if _, err := e2.Get(ctx, "a"); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("wrong key: want ErrDecrypt, got %v", err)
	}
}

func TestChunkKeyRoundTrip(t *testing.T) {
	id := "ab" + string(bytes.Repeat([]byte("0"), 62))
	k := ChunkKey(id)
	if k != "chunks/ab/"+id {
		t.Fatal(k)
	}
	if got, ok := ChunkIDFromKey(k); !ok || got != id {
		t.Fatal(got)
	}
}

// TestS3Conformance runs against a real endpoint when DFS_S3_ENDPOINT is set
// (scripts/dev-deps.sh up starts MinIO).
func TestS3Conformance(t *testing.T) {
	cfg, ok := S3ConfigFromEnv()
	if !ok {
		t.Skip("DFS_S3_ENDPOINT not set")
	}
	cfg.Bucket = cfg.Bucket + "-conformance"
	cfg.CreateBucket = true
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := NewS3(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.RemoveAll(context.Background())
	conformance(t, s)
}

func TestMain(m *testing.M) { os.Exit(m.Run()) }
