// Package metastore holds file metadata: which chunks make up each version of
// a file, plus the catalogue of all chunks (which the repair coordinator
// scans). Production uses Cassandra; tests and single-process runs use the
// in-memory store, and both pass the same conformance suite.
//
// Consistency model: chunks are immutable and written (to a replica write
// quorum) before the manifest that references them. A manifest is a single
// row, inserted atomically, so a reader sees either the complete previous
// version or the complete new one, never a partial file. Concurrent writers
// of the same name each create a version; the newest version wins.
package metastore

import (
	"context"
	"errors"
	"time"
)

var ErrNotFound = errors.New("metastore: not found")

type ChunkRef struct {
	ID   string
	Size int64
}

type File struct {
	Name    string
	Version string
	Size    int64
	SHA256  string
	Chunks  []ChunkRef
	Created time.Time
	Deleted bool
}

type Store interface {
	// PutChunks records chunks in the catalogue. Idempotent.
	PutChunks(ctx context.Context, chunks []ChunkRef) error
	// CommitFile stores a new version of f.Name. The store assigns Version
	// and Created and returns the stored record.
	CommitFile(ctx context.Context, f File) (File, error)
	// GetFile returns the given version, or the latest when version is "".
	// A deleted latest version is ErrNotFound.
	GetFile(ctx context.Context, name, version string) (File, error)
	// DeleteFile writes a tombstone version.
	DeleteFile(ctx context.Context, name string) error
	// ListFiles returns the latest live version of files whose name has the
	// prefix, sorted by name, up to limit (0 = no limit).
	ListFiles(ctx context.Context, prefix string, limit int) ([]File, error)
	// ScanChunks pages through the whole catalogue.
	ScanChunks(ctx context.Context, fn func(page []ChunkRef) error) error
	Close() error
}
