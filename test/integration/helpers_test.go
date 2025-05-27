// Package integration exercises a full in-process cluster: three Raft
// control-plane nodes and N storage nodes on loopback TCP, real gRPC between
// all of them, a shared metastore and a directory-backed (or MinIO) S3 tier.
// Set DFS_CASSANDRA / DFS_S3_ENDPOINT to run the same tests against real
// Cassandra and MinIO (scripts/dev-deps.sh up).
package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
)

// startCluster starts a cluster and registers cleanup. Backends come from the
// environment when set, else in-memory metastore and directory S3.
func startCluster(t *testing.T, opts cluster.Options) *cluster.Cluster {
	t.Helper()
	if opts.Dir == "" {
		opts.Dir = t.TempDir()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if opts.Metastore == nil {
		if hosts := os.Getenv("DFS_CASSANDRA"); hosts != "" {
			ks := fmt.Sprintf("dfs_it_%d", time.Now().UnixNano()%1e9)
			cs, err := metastore.NewCassandra(ctx, metastore.CassandraConfig{Hosts: strings.Split(hosts, ","), Keyspace: ks})
			if err != nil {
				t.Fatalf("cassandra: %v", err)
			}
			t.Cleanup(func() { cs.DropKeyspace(context.Background()); cs.Close() })
			opts.Metastore = cs
		}
	}
	if opts.Blob == nil && !opts.NoBlob {
		if cfg, ok := blob.S3ConfigFromEnv(); ok {
			cfg.Bucket = fmt.Sprintf("dfs-it-%d", time.Now().UnixNano()%1e9)
			cfg.CreateBucket = true
			s3, err := blob.NewS3(ctx, cfg)
			if err != nil {
				t.Fatalf("s3: %v", err)
			}
			key := make([]byte, 32)
			rand.Read(key)
			enc, _ := blob.NewEncrypted(s3, key)
			opts.Blob = enc
		}
	}
	c, err := cluster.Start(opts)
	if err != nil {
		t.Fatalf("start cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func sum(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func ctxT(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func download(t *testing.T, c *cluster.Cluster, ctx context.Context, name string) []byte {
	t.Helper()
	var buf bytes.Buffer
	if _, err := c.Client.Download(ctx, name, &buf); err != nil {
		t.Fatalf("download %s: %v", name, err)
	}
	return buf.Bytes()
}

// replicaCounts returns, for every chunk of the file, how many live nodes in
// its current preference list hold it.
func replicaCounts(t *testing.T, c *cluster.Cluster, ctx context.Context, name string) []int {
	t.Helper()
	f, err := c.Metastore().GetFile(ctx, name, "")
	if err != nil {
		t.Fatal(err)
	}
	r := c.LeaderRing()
	out := make([]int, len(f.Chunks))
	for i, ch := range f.Chunks {
		for _, m := range r.Lookup(ch.ID, 3) {
			idx := c.NodeByID(m.ID)
			if c.NodeUp(idx) && c.Nodes[idx].Store().Has(ch.ID) {
				out[i]++
			}
		}
	}
	return out
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) time.Duration {
	t.Helper()
	start := time.Now()
	for time.Since(start) < timeout {
		if cond() {
			return time.Since(start)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
	return 0
}
