package metastore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Memory is an in-process Store. Safe for concurrent use.
type Memory struct {
	mu       sync.RWMutex
	files    map[string][]File // versions, newest first
	chunks   map[string]int64
	lastNano int64
}

func NewMemory() *Memory {
	return &Memory{files: map[string][]File{}, chunks: map[string]int64{}}
}

func (m *Memory) PutChunks(ctx context.Context, chunks []ChunkRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, c := range chunks {
		m.chunks[c.ID] = c.Size
	}
	return nil
}

// nextVersion returns a strictly increasing, lexically sortable version.
func (m *Memory) nextVersion() (string, time.Time) {
	now := time.Now()
	n := now.UnixNano()
	if n <= m.lastNano {
		n = m.lastNano + 1
	}
	m.lastNano = n
	return fmt.Sprintf("%020d", n), time.Unix(0, n)
}

func (m *Memory) CommitFile(ctx context.Context, f File) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	f.Version, f.Created = m.nextVersion()
	f.Chunks = append([]ChunkRef(nil), f.Chunks...)
	m.files[f.Name] = append([]File{f}, m.files[f.Name]...)
	return f, nil
}

func (m *Memory) GetFile(ctx context.Context, name, version string) (File, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	vs := m.files[name]
	if len(vs) == 0 {
		return File{}, ErrNotFound
	}
	if version == "" {
		if vs[0].Deleted {
			return File{}, ErrNotFound
		}
		return clone(vs[0]), nil
	}
	for _, v := range vs {
		if v.Version == version {
			if v.Deleted {
				return File{}, ErrNotFound
			}
			return clone(v), nil
		}
	}
	return File{}, ErrNotFound
}

func (m *Memory) DeleteFile(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if vs := m.files[name]; len(vs) == 0 || vs[0].Deleted {
		return ErrNotFound
	}
	v, ts := m.nextVersion()
	m.files[name] = append([]File{{Name: name, Version: v, Created: ts, Deleted: true}}, m.files[name]...)
	return nil
}

func (m *Memory) ListFiles(ctx context.Context, prefix string, limit int) ([]File, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []File
	for name, vs := range m.files {
		if strings.HasPrefix(name, prefix) && len(vs) > 0 && !vs[0].Deleted {
			out = append(out, clone(vs[0]))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (m *Memory) ScanChunks(ctx context.Context, fn func([]ChunkRef) error) error {
	m.mu.RLock()
	all := make([]ChunkRef, 0, len(m.chunks))
	for id, sz := range m.chunks {
		all = append(all, ChunkRef{ID: id, Size: sz})
	}
	m.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	const page = 500
	for i := 0; i < len(all); i += page {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(all[i:min(i+page, len(all))]); err != nil {
			return err
		}
	}
	return nil
}

func (m *Memory) Close() error { return nil }

func clone(f File) File {
	f.Chunks = append([]ChunkRef(nil), f.Chunks...)
	return f
}
