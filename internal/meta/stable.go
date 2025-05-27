package meta

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// FileStableStore is a raft.StableStore that makes each Set durable with a
// single fsync. Raft writes its current term and vote here on the election
// path (three Sets per election on each voter), so the store's sync cost is
// paid directly in failover latency. BoltDB commits cost two syncs each
// (data pages, then the meta page), and on macOS every sync is an
// F_FULLFSYNC of several milliseconds.
//
// The whole key/value map is small (three keys), so every Set rewrites it
// into one of two slot files, alternating, as one record:
//
//	seq(8) | len(4) | crc32c(4) | json(len)
//
// A torn or partial write can only damage the slot being written; Open picks
// the valid record with the highest sequence number.
type FileStableStore struct {
	mu    sync.Mutex
	kv    map[string][]byte
	seq   uint64
	slots [2]*os.File
}

var errStableKeyNotFound = errors.New("not found") // raft checks for this text

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func OpenFileStableStore(dir string) (*FileStableStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStableStore{kv: map[string][]byte{}}
	created := false
	for i := range s.slots {
		p := filepath.Join(dir, fmt.Sprintf("stable.%d", i))
		if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
			created = true
		}
		f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			s.Close()
			return nil, err
		}
		s.slots[i] = f
	}
	if created {
		// Make the new directory entries durable once, up front.
		if d, err := os.Open(dir); err == nil {
			d.Sync()
			d.Close()
		}
	}
	var best *record
	for _, f := range s.slots {
		if r, err := readRecord(f); err == nil && (best == nil || r.seq > best.seq) {
			best = r
		}
	}
	if best != nil {
		s.seq = best.seq
		var m map[string][]byte
		if err := json.Unmarshal(best.data, &m); err != nil {
			s.Close()
			return nil, fmt.Errorf("stable store: decode: %w", err)
		}
		s.kv = m
	}
	return s, nil
}

type record struct {
	seq  uint64
	data []byte
}

func readRecord(f *os.File) (*record, error) {
	var hdr [16]byte
	if _, err := f.ReadAt(hdr[:], 0); err != nil {
		return nil, err
	}
	seq := binary.LittleEndian.Uint64(hdr[0:8])
	n := binary.LittleEndian.Uint32(hdr[8:12])
	sum := binary.LittleEndian.Uint32(hdr[12:16])
	if n > 1<<20 {
		return nil, errors.New("stable store: bad length")
	}
	data := make([]byte, n)
	if _, err := f.ReadAt(data, 16); err != nil && !(errors.Is(err, io.EOF) && n == 0) {
		return nil, err
	}
	if crc32.Checksum(append(hdr[0:12:12], data...), castagnoli) != sum {
		return nil, errors.New("stable store: checksum mismatch")
	}
	return &record{seq: seq, data: data}, nil
}

func (s *FileStableStore) persistLocked() error {
	data, err := json.Marshal(s.kv)
	if err != nil {
		return err
	}
	seq := s.seq + 1
	buf := make([]byte, 16+len(data))
	binary.LittleEndian.PutUint64(buf[0:8], seq)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(len(data)))
	copy(buf[16:], data)
	binary.LittleEndian.PutUint32(buf[12:16], crc32.Checksum(append(buf[0:12:12], data...), castagnoli))
	f := s.slots[seq%2]
	if _, err := f.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	s.seq = seq
	return nil
}

func (s *FileStableStore) Set(key, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, had := s.kv[string(key)]
	s.kv[string(key)] = append([]byte(nil), val...)
	if err := s.persistLocked(); err != nil {
		if had {
			s.kv[string(key)] = old
		} else {
			delete(s.kv, string(key))
		}
		return err
	}
	return nil
}

func (s *FileStableStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.kv[string(key)]
	if !ok {
		return nil, errStableKeyNotFound
	}
	return append([]byte(nil), v...), nil
}

func (s *FileStableStore) SetUint64(key []byte, val uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], val)
	return s.Set(key, b[:])
}

func (s *FileStableStore) GetUint64(key []byte) (uint64, error) {
	v, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, errors.New("stable store: bad uint64")
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *FileStableStore) Close() error {
	var first error
	for _, f := range s.slots {
		if f != nil {
			if err := f.Close(); err != nil && first == nil {
				first = err
			}
		}
	}
	return first
}
