package metastore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// CassandraConfig configures the Cassandra-backed store.
type CassandraConfig struct {
	Hosts       []string // host or host:port
	Keyspace    string
	Replication int // keyspace replication factor (SimpleStrategy)
	Consistency gocql.Consistency
	Timeout     time.Duration
}

// Cassandra stores manifests in a wide row per file name, clustered by a
// timeuuid version (newest first), and the chunk catalogue in 256 shards keyed
// by the first byte of the chunk ID so the repair scan can page shard by shard.
type Cassandra struct {
	s  *gocql.Session
	ks string
	cl gocql.Consistency
}

const chunkShards = 256

var keyspaceRE = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_]{0,47}$`)

func NewCassandra(ctx context.Context, cfg CassandraConfig) (*Cassandra, error) {
	if !keyspaceRE.MatchString(cfg.Keyspace) {
		return nil, fmt.Errorf("metastore: invalid keyspace %q", cfg.Keyspace)
	}
	if cfg.Replication <= 0 {
		cfg.Replication = 1
	}
	if cfg.Consistency == 0 {
		cfg.Consistency = gocql.Quorum
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Second
	}
	// gocql dials hosts it discovers (system.local/system.peers) on
	// ClusterConfig.Port, not on the contact point's port, so a non-default
	// port must be set there too or queries get routed to a dead address.
	hosts, port, err := splitHostsPort(cfg.Hosts)
	if err != nil {
		return nil, err
	}
	newCluster := func(ks string) *gocql.ClusterConfig {
		c := gocql.NewCluster(hosts...)
		if port != 0 {
			c.Port = port
		}
		c.Keyspace = ks
		c.Consistency = cfg.Consistency
		c.Timeout = cfg.Timeout
		c.ConnectTimeout = cfg.Timeout
		c.NumConns = 2
		c.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{NumRetries: 3, Min: 50 * time.Millisecond, Max: time.Second}
		return c
	}
	boot, err := newCluster("").CreateSession()
	if err != nil {
		return nil, fmt.Errorf("metastore: connect: %w", err)
	}
	stmts := []string{
		fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': %d}`, cfg.Keyspace, cfg.Replication),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.files (
			name text, version timeuuid, size bigint, sha256 text,
			chunk_ids list<text>, chunk_sizes list<bigint>, deleted boolean,
			PRIMARY KEY (name, version)
		) WITH CLUSTERING ORDER BY (version DESC)`, cfg.Keyspace),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.chunks (
			shard int, id text, size bigint, PRIMARY KEY (shard, id)
		)`, cfg.Keyspace),
	}
	for _, q := range stmts {
		if err := boot.Query(q).WithContext(ctx).Consistency(gocql.One).Exec(); err != nil {
			boot.Close()
			return nil, fmt.Errorf("metastore: schema: %w", err)
		}
	}
	boot.Close()
	s, err := newCluster(cfg.Keyspace).CreateSession()
	if err != nil {
		return nil, fmt.Errorf("metastore: connect keyspace: %w", err)
	}
	return &Cassandra{s: s, ks: cfg.Keyspace, cl: cfg.Consistency}, nil
}

func splitHostsPort(in []string) ([]string, int, error) {
	var hosts []string
	port := 0
	for _, h := range in {
		host, p, err := net.SplitHostPort(h)
		if err != nil {
			hosts = append(hosts, h) // no port
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, 0, fmt.Errorf("metastore: bad port in %q", h)
		}
		if port != 0 && n != port {
			return nil, 0, fmt.Errorf("metastore: all Cassandra hosts must use the same port")
		}
		port = n
		hosts = append(hosts, host)
	}
	return hosts, port, nil
}

func shardOf(id string) int {
	if len(id) < 2 {
		return 0
	}
	var b int
	fmt.Sscanf(id[:2], "%02x", &b)
	return b % chunkShards
}

func (c *Cassandra) PutChunks(ctx context.Context, chunks []ChunkRef) error {
	// Unlogged batches only help within one partition; with 256 shards the
	// chunks of a file spread out, so issue individual idempotent inserts,
	// several in flight at once.
	const inflight = 32
	sem := make(chan struct{}, inflight)
	errc := make(chan error, len(chunks))
	for _, ch := range chunks {
		sem <- struct{}{}
		go func(ch ChunkRef) {
			defer func() { <-sem }()
			errc <- c.s.Query(`INSERT INTO chunks (shard, id, size) VALUES (?, ?, ?)`,
				shardOf(ch.ID), ch.ID, ch.Size).WithContext(ctx).Idempotent(true).Exec()
		}(ch)
	}
	var first error
	for range chunks {
		if err := <-errc; err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (c *Cassandra) CommitFile(ctx context.Context, f File) (File, error) {
	ver := gocql.TimeUUID()
	ids := make([]string, len(f.Chunks))
	sizes := make([]int64, len(f.Chunks))
	for i, ch := range f.Chunks {
		ids[i], sizes[i] = ch.ID, ch.Size
	}
	if err := c.s.Query(`INSERT INTO files (name, version, size, sha256, chunk_ids, chunk_sizes, deleted) VALUES (?, ?, ?, ?, ?, ?, false)`,
		f.Name, ver, f.Size, f.SHA256, ids, sizes).WithContext(ctx).Exec(); err != nil {
		return File{}, err
	}
	f.Version = ver.String()
	f.Created = ver.Time()
	return f, nil
}

func (c *Cassandra) scanFile(sc interface{ Scan(...any) error }, name string) (File, error) {
	var (
		f     = File{Name: name}
		ver   gocql.UUID
		ids   []string
		sizes []int64
	)
	if err := sc.Scan(&ver, &f.Size, &f.SHA256, &ids, &sizes, &f.Deleted); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return File{}, ErrNotFound
		}
		return File{}, err
	}
	if len(ids) != len(sizes) {
		return File{}, fmt.Errorf("metastore: corrupt manifest for %q", name)
	}
	f.Version = ver.String()
	f.Created = ver.Time()
	f.Chunks = make([]ChunkRef, len(ids))
	for i := range ids {
		f.Chunks[i] = ChunkRef{ID: ids[i], Size: sizes[i]}
	}
	return f, nil
}

func (c *Cassandra) GetFile(ctx context.Context, name, version string) (File, error) {
	var q *gocql.Query
	if version == "" {
		q = c.s.Query(`SELECT version, size, sha256, chunk_ids, chunk_sizes, deleted FROM files WHERE name = ? LIMIT 1`, name)
	} else {
		ver, err := gocql.ParseUUID(version)
		if err != nil {
			return File{}, ErrNotFound
		}
		q = c.s.Query(`SELECT version, size, sha256, chunk_ids, chunk_sizes, deleted FROM files WHERE name = ? AND version = ?`, name, ver)
	}
	f, err := c.scanFile(q.WithContext(ctx), name)
	if err != nil {
		return File{}, err
	}
	if f.Deleted {
		return File{}, ErrNotFound
	}
	return f, nil
}

func (c *Cassandra) DeleteFile(ctx context.Context, name string) error {
	if _, err := c.GetFile(ctx, name, ""); err != nil {
		return err
	}
	return c.s.Query(`INSERT INTO files (name, version, size, sha256, chunk_ids, chunk_sizes, deleted) VALUES (?, ?, 0, '', [], [], true)`,
		name, gocql.TimeUUID()).WithContext(ctx).Exec()
}

func (c *Cassandra) ListFiles(ctx context.Context, prefix string, limit int) ([]File, error) {
	// SELECT DISTINCT over partition keys is a full scan; fine for the
	// catalogue sizes this project targets. A name index table would replace
	// it at larger scale.
	iter := c.s.Query(`SELECT DISTINCT name FROM files`).WithContext(ctx).PageSize(1000).Iter()
	var names []string
	var name string
	for iter.Scan(&name) {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	sort.Strings(names)
	var out []File
	for _, n := range names {
		f, err := c.GetFile(ctx, n, "")
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, f)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (c *Cassandra) ScanChunks(ctx context.Context, fn func([]ChunkRef) error) error {
	page := make([]ChunkRef, 0, 500)
	for shard := 0; shard < chunkShards; shard++ {
		iter := c.s.Query(`SELECT id, size FROM chunks WHERE shard = ?`, shard).WithContext(ctx).PageSize(500).Iter()
		var id string
		var size int64
		for iter.Scan(&id, &size) {
			page = append(page, ChunkRef{ID: id, Size: size})
			if len(page) == cap(page) {
				if err := fn(page); err != nil {
					iter.Close()
					return err
				}
				page = page[:0]
			}
		}
		if err := iter.Close(); err != nil {
			return err
		}
	}
	if len(page) > 0 {
		return fn(page)
	}
	return nil
}

// DropKeyspace is for tests.
func (c *Cassandra) DropKeyspace(ctx context.Context) error {
	return c.s.Query(fmt.Sprintf(`DROP KEYSPACE IF EXISTS %s`, c.ks)).WithContext(ctx).Consistency(gocql.One).Exec()
}

func (c *Cassandra) Close() error {
	c.s.Close()
	return nil
}
