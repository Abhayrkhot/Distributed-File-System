// Package cas is a storage node's local content-addressed chunk store.
//
// It grew out of the original prototype's CAS path transform (hash the key,
// shard the hex digest into directories). The changes: keys are SHA-256
// digests of the content itself, so the store verifies every write against
// its ID and a partially written or corrupted chunk never becomes visible.
// Writes go to a temp file and are renamed into place.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrNotFound     = errors.New("cas: chunk not found")
	ErrHashMismatch = errors.New("cas: content does not match chunk id")
	ErrInvalidID    = errors.New("cas: invalid chunk id")
	ErrSizeMismatch = errors.New("cas: content size does not match header")
)

// ID returns the chunk ID (hex SHA-256) of b.
func ID(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

// ValidID reports whether id looks like a hex SHA-256 digest.
func ValidID(id string) bool {
	if len(id) != 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// Options configure a Store.
type Options struct {
	Root string
	// Fsync each chunk before it becomes visible. Durable, slower.
	Fsync bool
}

// Store is safe for concurrent use.
type Store struct {
	opts   Options
	chunks atomic.Int64
	bytes  atomic.Int64
	// Serialises the rename step per chunk so stats stay exact when the same
	// chunk is written concurrently by two coordinators.
	locks [64]sync.Mutex
}

// Open opens (creating if needed) a store rooted at opts.Root and counts the
// chunks already on disk.
func Open(opts Options) (*Store, error) {
	if opts.Root == "" {
		return nil, errors.New("cas: empty root")
	}
	if err := os.MkdirAll(filepath.Join(opts.Root, "tmp"), 0o755); err != nil {
		return nil, err
	}
	s := &Store{opts: opts}
	// Leftover temp files are from writes that never completed.
	_ = os.RemoveAll(filepath.Join(opts.Root, "tmp"))
	if err := os.MkdirAll(filepath.Join(opts.Root, "tmp"), 0o755); err != nil {
		return nil, err
	}
	err := s.Walk(func(id string, size int64) error {
		s.chunks.Add(1)
		s.bytes.Add(size)
		return nil
	})
	return s, err
}

// Root returns the store's directory.
func (s *Store) Root() string { return s.opts.Root }

func (s *Store) path(id string) string {
	return filepath.Join(s.opts.Root, "chunks", id[0:2], id[2:4], id)
}

func (s *Store) lock(id string) *sync.Mutex {
	b, _ := hex.DecodeString(id[:2])
	return &s.locks[int(b[0])%len(s.locks)]
}

// Has reports whether the chunk is stored locally.
func (s *Store) Has(id string) bool {
	if !ValidID(id) {
		return false
	}
	_, err := os.Stat(s.path(id))
	return err == nil
}

// Stat returns the stored size of a chunk.
func (s *Store) Stat(id string) (int64, error) {
	if !ValidID(id) {
		return 0, ErrInvalidID
	}
	fi, err := os.Stat(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// Put streams r into the store under id. size < 0 means unknown. If the chunk
// already exists the reader is drained and alreadyPresent is true. The write is
// rejected unless the content hashes to id.
func (s *Store) Put(id string, size int64, r io.Reader) (written int64, alreadyPresent bool, err error) {
	if !ValidID(id) {
		return 0, false, ErrInvalidID
	}
	if s.Has(id) {
		n, err := io.Copy(io.Discard, r)
		return n, true, err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.opts.Root, "tmp"), id[:8]+"-*")
	if err != nil {
		return 0, false, err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(tmp, h), r)
	if err != nil {
		return n, false, err
	}
	if size >= 0 && n != size {
		return n, false, fmt.Errorf("%w: header %d, got %d", ErrSizeMismatch, size, n)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != id {
		return n, false, fmt.Errorf("%w: want %s, got %s", ErrHashMismatch, id, got)
	}
	if s.opts.Fsync {
		if err = tmp.Sync(); err != nil {
			return n, false, err
		}
	}
	if err = tmp.Close(); err != nil {
		return n, false, err
	}
	final := s.path(id)
	if err = os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return n, false, err
	}
	mu := s.lock(id)
	mu.Lock()
	defer mu.Unlock()
	if s.Has(id) {
		os.Remove(tmpName)
		return n, true, nil
	}
	if err = os.Rename(tmpName, final); err != nil {
		return n, false, err
	}
	s.chunks.Add(1)
	s.bytes.Add(n)
	return n, false, nil
}

// PutBytes is Put for an in-memory chunk.
func (s *Store) PutBytes(id string, b []byte) (alreadyPresent bool, err error) {
	_, already, err := s.Put(id, int64(len(b)), &byteReader{b: b})
	return already, err
}

// Open returns a reader for a chunk and its size. The caller closes it.
func (s *Store) Open(id string) (io.ReadCloser, int64, error) {
	if !ValidID(id) {
		return nil, 0, ErrInvalidID
	}
	f, err := os.Open(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

// ReadAll reads a whole chunk and re-verifies its hash (detects bit rot).
func (s *Store) ReadAll(id string) ([]byte, error) {
	rc, size, err := s.Open(id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	b := make([]byte, size)
	if _, err := io.ReadFull(rc, b); err != nil {
		return nil, err
	}
	if ID(b) != id {
		return nil, ErrHashMismatch
	}
	return b, nil
}

// Delete removes a chunk. Deleting a missing chunk is not an error.
func (s *Store) Delete(id string) error {
	if !ValidID(id) {
		return ErrInvalidID
	}
	mu := s.lock(id)
	mu.Lock()
	defer mu.Unlock()
	fi, err := os.Stat(s.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.Remove(s.path(id)); err != nil {
		return err
	}
	s.chunks.Add(-1)
	s.bytes.Add(-fi.Size())
	return nil
}

// Walk calls fn for every stored chunk.
func (s *Store) Walk(fn func(id string, size int64) error) error {
	root := filepath.Join(s.opts.Root, "chunks")
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() || !ValidID(d.Name()) || strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		return fn(d.Name(), fi.Size())
	})
	return err
}

// Stats returns the chunk count and total bytes held.
func (s *Store) Stats() (chunks, bytes int64) { return s.chunks.Load(), s.bytes.Load() }

// Verifier wraps a writer and checks a chunk's hash once all bytes passed.
type Verifier struct {
	id string
	h  hash.Hash
	n  int64
}

func NewVerifier(id string) *Verifier { return &Verifier{id: id, h: sha256.New()} }

func (v *Verifier) Write(p []byte) (int, error) {
	v.n += int64(len(p))
	return v.h.Write(p)
}

// Check returns ErrHashMismatch unless the bytes written hash to the ID.
func (v *Verifier) Check() error {
	if hex.EncodeToString(v.h.Sum(nil)) != v.id {
		return ErrHashMismatch
	}
	return nil
}

func (v *Verifier) N() int64 { return v.n }

type byteReader struct {
	b []byte
	i int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

// WriteTo lets io.Copy hand the whole slice to the destination at once.
func (r *byteReader) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(r.b[r.i:])
	r.i += n
	return int64(n), err
}
