package meta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileStableStore(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenFileStableStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get([]byte("x")); err == nil || err.Error() != "not found" {
		t.Fatalf("missing key: %v", err)
	}
	if v, err := s.GetUint64([]byte("CurrentTerm")); err == nil || v != 0 {
		t.Fatalf("missing uint: %v %v", v, err)
	}
	for term := uint64(1); term <= 5; term++ {
		if err := s.SetUint64([]byte("CurrentTerm"), term); err != nil {
			t.Fatal(err)
		}
		if err := s.Set([]byte("LastVoteCand"), []byte("meta-1")); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()

	s2, err := OpenFileStableStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := s2.GetUint64([]byte("CurrentTerm")); v != 5 {
		t.Fatalf("term after reopen: %d", v)
	}
	if v, _ := s2.Get([]byte("LastVoteCand")); string(v) != "meta-1" {
		t.Fatalf("vote after reopen: %q", v)
	}
	s2.Close()
}

// A torn write to the newest slot must fall back to the previous record.
func TestFileStableStoreTornWrite(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenFileStableStore(dir)
	s.SetUint64([]byte("CurrentTerm"), 7) // seq 1 -> slot 1
	s.SetUint64([]byte("CurrentTerm"), 8) // seq 2 -> slot 0
	s.Close()
	// Corrupt slot 0 (the newest record).
	p := filepath.Join(dir, "stable.0")
	b, _ := os.ReadFile(p)
	b[len(b)-1] ^= 0xff
	os.WriteFile(p, b, 0o644)
	s2, err := OpenFileStableStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if v, _ := s2.GetUint64([]byte("CurrentTerm")); v != 7 {
		t.Fatalf("want fallback to 7, got %d", v)
	}
}
