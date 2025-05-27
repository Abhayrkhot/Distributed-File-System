// Package blob is the cold, durable tier behind the storage nodes: an
// S3-compatible object store holding one object per chunk. Storage nodes write
// chunks here behind the replica write (write-behind), and the repair
// coordinator restores chunks from here when every on-node replica is gone.
package blob

import (
	"context"
	"errors"
	"strings"
)

var ErrNotFound = errors.New("blob: not found")

// Store is the minimal object-store surface the system needs. Chunks are at
// most a few MiB, so the API moves whole objects.
type Store interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
	// List calls fn for every key under prefix, in lexical order.
	List(ctx context.Context, prefix string, fn func(key string) error) error
	Delete(ctx context.Context, key string) error
}

// ChunkPrefix is where chunk objects live.
const ChunkPrefix = "chunks/"

// ChunkKey maps a chunk ID to its object key. The two-character shard keeps
// listings and S3 request partitions spread out.
func ChunkKey(id string) string { return ChunkPrefix + id[:2] + "/" + id }

// ChunkIDFromKey is the inverse of ChunkKey.
func ChunkIDFromKey(key string) (string, bool) {
	if !strings.HasPrefix(key, ChunkPrefix) {
		return "", false
	}
	i := strings.LastIndexByte(key, '/')
	id := key[i+1:]
	if len(id) != 64 {
		return "", false
	}
	return id, true
}
