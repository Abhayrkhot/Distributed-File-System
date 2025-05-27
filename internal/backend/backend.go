// Package backend wires command-line flags (with environment fallbacks) to
// the metastore and S3-tier implementations, so every binary configures
// Cassandra and S3 the same way.
package backend

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/gocql/gocql"

	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
)

type Flags struct {
	Cassandra   string
	Keyspace    string
	CassandraRF int
	Consistency string
	SkipSchema  bool
	MemoryMeta  bool
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3Region    string
	S3TLS       bool
	S3EncKeyHex string
	BlobDir     string
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Register adds the backend flags to fs.
func (f *Flags) Register(fs *flag.FlagSet) {
	fs.StringVar(&f.Cassandra, "cassandra", env("DFS_CASSANDRA", ""), "Cassandra contact points, comma separated host[:port] (env DFS_CASSANDRA)")
	fs.StringVar(&f.Keyspace, "keyspace", env("DFS_KEYSPACE", "dfs"), "Cassandra keyspace")
	fs.IntVar(&f.CassandraRF, "cassandra-rf", 1, "keyspace replication factor when creating it")
	fs.StringVar(&f.Consistency, "consistency", env("DFS_CONSISTENCY", "QUORUM"), "Cassandra consistency level")
	fs.BoolVar(&f.MemoryMeta, "memory-metastore", false, "use an in-process metastore (single process only)")
	fs.StringVar(&f.S3Endpoint, "s3-endpoint", env("DFS_S3_ENDPOINT", ""), "S3 endpoint host:port (env DFS_S3_ENDPOINT); empty disables the S3 tier")
	fs.StringVar(&f.S3Bucket, "s3-bucket", env("DFS_S3_BUCKET", "dfs-chunks"), "S3 bucket")
	fs.StringVar(&f.S3AccessKey, "s3-access-key", env("DFS_S3_ACCESS_KEY", ""), "S3 access key (env DFS_S3_ACCESS_KEY)")
	fs.StringVar(&f.S3SecretKey, "s3-secret-key", env("DFS_S3_SECRET_KEY", ""), "S3 secret key (env DFS_S3_SECRET_KEY)")
	fs.StringVar(&f.S3Region, "s3-region", env("DFS_S3_REGION", ""), "S3 region")
	fs.BoolVar(&f.S3TLS, "s3-tls", env("DFS_S3_TLS", "") == "1", "use TLS for S3")
	fs.StringVar(&f.S3EncKeyHex, "s3-encryption-key", env("DFS_S3_ENCRYPTION_KEY", ""), "hex 32-byte key: encrypt chunks in S3 with AES-256-GCM (env DFS_S3_ENCRYPTION_KEY)")
	fs.StringVar(&f.BlobDir, "blob-dir", env("DFS_BLOB_DIR", ""), "use a local directory instead of S3 for the cold tier")
}

// Metastore opens the configured metastore. Returns nil, nil if none is
// configured.
func (f *Flags) Metastore(ctx context.Context, log *slog.Logger) (metastore.Store, error) {
	if f.MemoryMeta {
		return metastore.NewMemory(), nil
	}
	if f.Cassandra == "" {
		return nil, nil
	}
	cl, err := parseConsistency(f.Consistency)
	if err != nil {
		return nil, err
	}
	log.Info("connecting to cassandra", "hosts", f.Cassandra, "keyspace", f.Keyspace, "consistency", cl)
	return metastore.NewCassandra(ctx, metastore.CassandraConfig{
		Hosts: strings.Split(f.Cassandra, ","), Keyspace: f.Keyspace, Replication: f.CassandraRF, Consistency: cl,
	})
}

// Blob opens the configured cold tier. Returns nil, nil if none.
func (f *Flags) Blob(ctx context.Context, log *slog.Logger) (blob.Store, error) {
	var (
		s   blob.Store
		err error
	)
	switch {
	case f.S3Endpoint != "":
		log.Info("using S3 tier", "endpoint", f.S3Endpoint, "bucket", f.S3Bucket)
		s, err = blob.NewS3(ctx, blob.S3Config{
			Endpoint: f.S3Endpoint, Bucket: f.S3Bucket, AccessKey: f.S3AccessKey, SecretKey: f.S3SecretKey,
			Region: f.S3Region, UseTLS: f.S3TLS, CreateBucket: true,
		})
	case f.BlobDir != "":
		s, err = blob.NewFS(f.BlobDir)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if f.S3EncKeyHex != "" {
		key, err := hex.DecodeString(f.S3EncKeyHex)
		if err != nil {
			return nil, fmt.Errorf("s3-encryption-key: %w", err)
		}
		return blob.NewEncrypted(s, key)
	}
	return s, nil
}

func parseConsistency(s string) (gocql.Consistency, error) {
	switch strings.ToUpper(s) {
	case "ONE":
		return gocql.One, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "ALL":
		return gocql.All, nil
	}
	return 0, fmt.Errorf("unsupported consistency %q", s)
}

// Logger builds the process logger.
func Logger(level string) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
