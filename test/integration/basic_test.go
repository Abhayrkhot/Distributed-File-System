package integration

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
)

const testChunk = 256 << 10 // small chunks so tests exercise many of them

func TestRoundTripSizes(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	sizes := []int{0, 1, testChunk - 1, testChunk, testChunk + 1, 10*testChunk + 12345}
	for _, n := range sizes {
		name := fmt.Sprintf("size-%d", n)
		data := randomBytes(n)
		fi, err := c.Client.UploadBytes(ctx, name, data)
		if err != nil {
			t.Fatalf("upload %d bytes: %v", n, err)
		}
		if fi.Size != int64(n) || fi.Sha256 != sum(data) {
			t.Fatalf("upload %d: info %+v", n, fi)
		}
		if got := download(t, c, ctx, name); !bytes.Equal(got, data) {
			t.Fatalf("download %d: content differs (%d bytes back)", n, len(got))
		}
	}
	files, err := c.Client.List(ctx, "size-", 0)
	if err != nil || len(files) != len(sizes) {
		t.Fatalf("list: %d files, %v", len(files), err)
	}
}

// Every chunk lands on exactly its N=3 preference-list nodes.
func TestReplicationFactor(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 8, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	data := randomBytes(40 * testChunk)
	if _, err := c.Client.UploadBytes(ctx, "rf.bin", data); err != nil {
		t.Fatal(err)
	}
	f, _ := c.Metastore().GetFile(ctx, "rf.bin", "")
	if len(f.Chunks) != 40 {
		t.Fatalf("want 40 chunks, got %d", len(f.Chunks))
	}
	// Writes return after W=2; the third replica lands asynchronously.
	waitFor(t, 5*time.Second, "all chunks at RF=3", func() bool {
		for _, ch := range f.Chunks {
			if len(c.Holders(ch.ID)) != 3 {
				return false
			}
		}
		return true
	})
	r := c.LeaderRing()
	for _, ch := range f.Chunks {
		want := map[int]bool{}
		for _, m := range r.Lookup(ch.ID, 3) {
			want[c.NodeByID(m.ID)] = true
		}
		for _, h := range c.Holders(ch.ID) {
			if !want[h] {
				t.Fatalf("chunk %s on node %d outside its preference list", ch.ID[:8], h)
			}
		}
	}
}

func TestDeleteAndVersions(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 4, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	v1, v2 := randomBytes(3*testChunk), randomBytes(2*testChunk)
	if _, err := c.Client.UploadBytes(ctx, "doc", v1); err != nil {
		t.Fatal(err)
	}
	fi2, err := c.Client.UploadBytes(ctx, "doc", v2)
	if err != nil {
		t.Fatal(err)
	}
	if got := download(t, c, ctx, "doc"); !bytes.Equal(got, v2) {
		t.Fatal("latest version not served")
	}
	st, err := c.Client.Stat(ctx, "doc")
	if err != nil || st.Version != fi2.Version || len(st.Chunks) != 2 {
		t.Fatalf("stat: %+v %v", st, err)
	}
	if err := c.Client.Delete(ctx, "doc"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Client.Stat(ctx, "doc"); err == nil {
		t.Fatal("stat after delete should fail")
	}
}

// Identical content is stored once per replica (content addressing dedups).
func TestDedup(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 4, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	data := randomBytes(4 * testChunk)
	for i := 0; i < 5; i++ {
		if _, err := c.Client.UploadBytes(ctx, fmt.Sprintf("copy-%d", i), data); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "replicas settle", func() bool {
		var chunks int64
		for _, n := range c.Nodes {
			k, _ := n.Store().Stats()
			chunks += k
		}
		return chunks == 4*3
	})
}
