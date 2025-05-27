package metastore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func conformance(t *testing.T, s Store) {
	ctx := context.Background()

	t.Run("missing", func(t *testing.T) {
		if _, err := s.GetFile(ctx, "nope", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		if err := s.DeleteFile(ctx, "nope"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("delete missing: want ErrNotFound, got %v", err)
		}
	})

	t.Run("versions", func(t *testing.T) {
		v1, err := s.CommitFile(ctx, File{Name: "a.txt", Size: 3, SHA256: "h1", Chunks: []ChunkRef{{ID: "c1", Size: 3}}})
		if err != nil {
			t.Fatal(err)
		}
		v2, err := s.CommitFile(ctx, File{Name: "a.txt", Size: 6, SHA256: "h2", Chunks: []ChunkRef{{ID: "c2", Size: 4}, {ID: "c3", Size: 2}}})
		if err != nil {
			t.Fatal(err)
		}
		if v1.Version == v2.Version || v1.Version == "" {
			t.Fatalf("versions not distinct: %q %q", v1.Version, v2.Version)
		}
		latest, err := s.GetFile(ctx, "a.txt", "")
		if err != nil {
			t.Fatal(err)
		}
		if latest.Version != v2.Version || latest.SHA256 != "h2" || len(latest.Chunks) != 2 || latest.Chunks[1].ID != "c3" {
			t.Fatalf("latest: %+v", latest)
		}
		old, err := s.GetFile(ctx, "a.txt", v1.Version)
		if err != nil || old.SHA256 != "h1" {
			t.Fatalf("old version: %+v %v", old, err)
		}
		if err := s.DeleteFile(ctx, "a.txt"); err != nil {
			t.Fatal(err)
		}
		if _, err := s.GetFile(ctx, "a.txt", ""); !errors.Is(err, ErrNotFound) {
			t.Fatalf("after delete: %v", err)
		}
	})

	t.Run("list", func(t *testing.T) {
		for _, n := range []string{"dir/b", "dir/a", "other/c", "dir/gone"} {
			if _, err := s.CommitFile(ctx, File{Name: n, Size: 1, SHA256: n}); err != nil {
				t.Fatal(err)
			}
		}
		s.DeleteFile(ctx, "dir/gone")
		got, err := s.ListFiles(ctx, "dir/", 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Name != "dir/a" || got[1].Name != "dir/b" {
			t.Fatalf("list: %+v", got)
		}
		if got, _ := s.ListFiles(ctx, "dir/", 1); len(got) != 1 {
			t.Fatalf("limit: %d", len(got))
		}
	})

	t.Run("chunks", func(t *testing.T) {
		var want []ChunkRef
		for i := 0; i < 1234; i++ {
			want = append(want, ChunkRef{ID: fmt.Sprintf("%064x", i*7919), Size: int64(i)})
		}
		if err := s.PutChunks(ctx, want); err != nil {
			t.Fatal(err)
		}
		if err := s.PutChunks(ctx, want[:10]); err != nil { // idempotent
			t.Fatal(err)
		}
		seen := map[string]int64{}
		if err := s.ScanChunks(ctx, func(p []ChunkRef) error {
			for _, c := range p {
				seen[c.ID] = c.Size
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, c := range want {
			if sz, ok := seen[c.ID]; !ok || sz != c.Size {
				t.Fatalf("chunk %s missing from scan", c.ID)
			}
		}
	})

	t.Run("concurrent writers", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				if _, err := s.CommitFile(ctx, File{Name: "race", Size: int64(i), SHA256: fmt.Sprint(i)}); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wg.Wait()
		f, err := s.GetFile(ctx, "race", "")
		if err != nil {
			t.Fatal(err)
		}
		if fmt.Sprint(f.Size) != f.SHA256 {
			t.Fatalf("torn manifest: %+v", f)
		}
	})
}

func TestMemoryConformance(t *testing.T) { conformance(t, NewMemory()) }

// TestCassandraConformance runs when DFS_CASSANDRA is set (scripts/dev-deps.sh up).
func TestCassandraConformance(t *testing.T) {
	hosts := os.Getenv("DFS_CASSANDRA")
	if hosts == "" {
		t.Skip("DFS_CASSANDRA not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s, err := NewCassandra(ctx, CassandraConfig{
		Hosts:    strings.Split(hosts, ","),
		Keyspace: fmt.Sprintf("dfs_test_%d", time.Now().UnixNano()%1e9),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.DropKeyspace(context.Background())
		s.Close()
	}()
	conformance(t, s)
}
