package cas

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestPutGetDelete(t *testing.T) {
	s := open(t)
	data := bytes.Repeat([]byte("hello"), 1000)
	id := ID(data)
	if s.Has(id) {
		t.Fatal("unexpected chunk")
	}
	n, already, err := s.Put(id, int64(len(data)), bytes.NewReader(data))
	if err != nil || already || n != int64(len(data)) {
		t.Fatalf("put: n=%d already=%v err=%v", n, already, err)
	}
	if !s.Has(id) {
		t.Fatal("missing after put")
	}
	got, err := s.ReadAll(id)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("read: %v", err)
	}
	if c, b := s.Stats(); c != 1 || b != int64(len(data)) {
		t.Fatalf("stats %d %d", c, b)
	}
	if _, already, _ := s.Put(id, -1, bytes.NewReader(data)); !already {
		t.Fatal("second put should report already present")
	}
	if c, _ := s.Stats(); c != 1 {
		t.Fatalf("duplicate counted: %d", c)
	}
	if err := s.Delete(id); err != nil {
		t.Fatal(err)
	}
	if s.Has(id) {
		t.Fatal("present after delete")
	}
	if _, _, err := s.Open(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	if c, b := s.Stats(); c != 0 || b != 0 {
		t.Fatalf("stats after delete %d %d", c, b)
	}
}

func TestRejectsWrongContent(t *testing.T) {
	s := open(t)
	id := ID([]byte("right"))
	if _, _, err := s.Put(id, -1, bytes.NewReader([]byte("wrong"))); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want hash mismatch, got %v", err)
	}
	if s.Has(id) {
		t.Fatal("corrupt chunk became visible")
	}
	if _, _, err := s.Put(id, 99, bytes.NewReader([]byte("right"))); !errors.Is(err, ErrSizeMismatch) {
		t.Fatalf("want size mismatch, got %v", err)
	}
	if _, _, err := s.Put("../../etc/passwd", -1, bytes.NewReader(nil)); !errors.Is(err, ErrInvalidID) {
		t.Fatalf("want invalid id, got %v", err)
	}
}

type failingReader struct{ n int }

func (f *failingReader) Read(p []byte) (int, error) {
	if f.n <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	k := min(len(p), f.n)
	f.n -= k
	return k, nil
}

func TestTruncatedWriteInvisible(t *testing.T) {
	s := open(t)
	id := ID(make([]byte, 4096))
	if _, _, err := s.Put(id, 4096, &failingReader{n: 100}); err == nil {
		t.Fatal("expected error")
	}
	if s.Has(id) {
		t.Fatal("partial chunk visible")
	}
	entries, _ := os.ReadDir(filepath.Join(s.Root(), "tmp"))
	if len(entries) != 0 {
		t.Fatalf("temp file leaked: %d", len(entries))
	}
}

func TestReopenCountsExisting(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(Options{Root: dir})
	for i := 0; i < 10; i++ {
		b := []byte(fmt.Sprintf("chunk-%d", i))
		if _, err := s.PutBytes(ID(b), b); err != nil {
			t.Fatal(err)
		}
	}
	s2, err := Open(Options{Root: dir})
	if err != nil {
		t.Fatal(err)
	}
	if c, _ := s2.Stats(); c != 10 {
		t.Fatalf("want 10 chunks after reopen, got %d", c)
	}
}

func TestConcurrentSameChunk(t *testing.T) {
	s := open(t)
	data := bytes.Repeat([]byte{7}, 1<<16)
	id := ID(data)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := s.Put(id, int64(len(data)), bytes.NewReader(data)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if c, b := s.Stats(); c != 1 || b != int64(len(data)) {
		t.Fatalf("stats after concurrent puts: %d %d", c, b)
	}
}

func TestBitRotDetected(t *testing.T) {
	s := open(t)
	b := []byte("precious bytes")
	id := ID(b)
	s.PutBytes(id, b)
	if err := os.WriteFile(s.path(id), []byte("precious bytez"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReadAll(id); !errors.Is(err, ErrHashMismatch) {
		t.Fatalf("want mismatch, got %v", err)
	}
}
