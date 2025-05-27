// Package cluster runs a whole system in one process for integration tests
// and demos: a Raft control plane plus N storage nodes, each on its own
// loopback TCP port, talking real gRPC and real Raft. Nodes can be killed
// (abrupt stop, data kept) and restarted on the same address and data dir.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/client"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
	"github.com/Abhayrkhot/Distributed-File-System/internal/storage"
)

type Options struct {
	Dir         string // required: root for all node state
	MetaNodes   int    // default 3
	Storage     int    // default 6
	Timing      meta.Timing
	DeadAfter   time.Duration // default 1s
	Replicas    int
	WriteQuorum int
	ChunkSize   int
	Metastore   metastore.Store // default in-memory
	Blob        blob.Store      // default directory-backed, under Dir
	// RepairInterval for the periodic pass; tests mostly trigger repair
	// explicitly or rely on the epoch-change trigger.
	RepairInterval time.Duration
	// ManualRepair disables epoch-triggered and periodic repair.
	ManualRepair bool
	// NoBlob runs without an S3 tier.
	NoBlob bool
	Logger       *slog.Logger
}

type Cluster struct {
	opts Options

	mu       sync.Mutex
	Metas    []*meta.Server
	metaUp   []bool
	Peers    []meta.Peer
	Nodes    []*storage.Node
	nodeUp   []bool
	nodeAddr []string

	MetaClient *meta.Client
	Client     *client.Client
}

func Start(opts Options) (*Cluster, error) {
	if opts.Dir == "" {
		return nil, errors.New("cluster: Dir is required")
	}
	if opts.MetaNodes == 0 {
		opts.MetaNodes = 3
	}
	if opts.Storage == 0 {
		opts.Storage = 6
	}
	if opts.Timing == (meta.Timing{}) {
		opts.Timing = meta.FastTiming
	}
	if opts.DeadAfter == 0 {
		opts.DeadAfter = time.Second
	}
	if opts.Replicas == 0 {
		opts.Replicas = 3
	}
	if opts.Metastore == nil {
		opts.Metastore = metastore.NewMemory()
	}
	if opts.Blob == nil && !opts.NoBlob {
		b, err := blob.NewFS(filepath.Join(opts.Dir, "s3"))
		if err != nil {
			return nil, err
		}
		opts.Blob = b
	}
	if opts.RepairInterval == 0 {
		opts.RepairInterval = time.Hour
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		if os.Getenv("DFS_TEST_LOG") != "" {
			opts.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
		}
	}
	c := &Cluster{opts: opts}

	for i := 0; i < opts.MetaNodes; i++ {
		m, err := c.openMeta(i, "127.0.0.1:0", "127.0.0.1:0")
		if err != nil {
			c.Close()
			return nil, err
		}
		c.Metas = append(c.Metas, m)
		c.metaUp = append(c.metaUp, true)
		c.Peers = append(c.Peers, meta.Peer{ID: m.ID(), RaftAddr: m.RaftAddr(), GRPCAddr: m.GRPCAddr()})
	}
	for _, m := range c.Metas {
		if err := m.Start(c.Peers, true); err != nil {
			c.Close()
			return nil, err
		}
	}
	var metaAddrs []string
	for _, p := range c.Peers {
		metaAddrs = append(metaAddrs, p.GRPCAddr)
	}
	c.MetaClient = meta.NewClient(metaAddrs, nil)
	if _, err := c.WaitLeader(10 * time.Second); err != nil {
		c.Close()
		return nil, err
	}

	for i := 0; i < opts.Storage; i++ {
		n, err := c.openNode(i, "127.0.0.1:0")
		if err != nil {
			c.Close()
			return nil, err
		}
		c.Nodes = append(c.Nodes, n)
		c.nodeUp = append(c.nodeUp, true)
		c.nodeAddr = append(c.nodeAddr, n.Addr())
		n.Start()
	}
	c.Client = client.New(c.nodeAddr)
	if err := c.WaitRingSize(opts.Storage, 15*time.Second); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (c *Cluster) openMeta(i int, raftBind, grpcBind string) (*meta.Server, error) {
	return meta.Open(meta.Config{
		ID:              fmt.Sprintf("meta-%d", i),
		RaftBind:        raftBind,
		GRPCBind:        grpcBind,
		DataDir:         filepath.Join(c.opts.Dir, fmt.Sprintf("meta-%d", i)),
		LogStore:        "bolt", // with NoSync: fast, non-durable test clusters
		NoSync:          true,
		FastCrashDetect: true,
		Timing:          c.opts.Timing,
		DeadAfter:       c.opts.DeadAfter,
		Replicas:        c.opts.Replicas,
		Metastore:       c.opts.Metastore,
		Blob:            c.opts.Blob,
		RepairInterval:  c.opts.RepairInterval,
		RepairDebounce:  200 * time.Millisecond,
		ManualRepair:    c.opts.ManualRepair,
		Logger:          c.opts.Logger,
		RaftLevel:       "error",
	})
}

func (c *Cluster) openNode(i int, bind string) (*storage.Node, error) {
	var metaAddrs []string
	for _, p := range c.Peers {
		metaAddrs = append(metaAddrs, p.GRPCAddr)
	}
	return storage.Open(storage.Config{
		ID:                fmt.Sprintf("node-%02d", i),
		Bind:              bind,
		DataDir:           filepath.Join(c.opts.Dir, fmt.Sprintf("node-%02d", i)),
		MetaAddrs:         metaAddrs,
		Metastore:         c.opts.Metastore,
		Blob:              c.opts.Blob,
		HeartbeatInterval: 100 * time.Millisecond,
		Replicas:          c.opts.Replicas,
		WriteQuorum:       c.opts.WriteQuorum,
		ChunkSize:         c.opts.ChunkSize,
		Logger:            c.opts.Logger,
	})
}

// Leader returns the index of the current leader, or -1.
func (c *Cluster) Leader() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, m := range c.Metas {
		if c.metaUp[i] && m.IsLeader() {
			return i
		}
	}
	return -1
}

func (c *Cluster) WaitLeader(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if i := c.Leader(); i >= 0 {
			return i, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return -1, errors.New("cluster: no leader elected")
}

// LeaderRing is the ring in the leader's applied state.
func (c *Cluster) LeaderRing() *ring.Ring {
	i := c.Leader()
	if i < 0 {
		return nil
	}
	return c.Metas[i].FSM().Ring()
}

// WaitRing waits until the leader's ring and every live storage node's ring
// satisfy pred and agree on the epoch.
func (c *Cluster) WaitRing(pred func(*ring.Ring) bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r := c.LeaderRing(); r != nil && pred(r) {
			ok := true
			c.mu.Lock()
			for i, n := range c.Nodes {
				if c.nodeUp[i] && n.Ring().Epoch() != r.Epoch() {
					ok = false
				}
			}
			c.mu.Unlock()
			if ok {
				return nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	r := c.LeaderRing()
	if r == nil {
		return errors.New("cluster: ring wait timed out (no leader)")
	}
	return fmt.Errorf("cluster: ring wait timed out (leader ring epoch %d, %d members)", r.Epoch(), r.Len())
}

func (c *Cluster) WaitRingSize(n int, timeout time.Duration) error {
	return c.WaitRing(func(r *ring.Ring) bool { return r.Len() == n }, timeout)
}

// WaitNodeOut waits until node i has left the ring (marked DOWN).
func (c *Cluster) WaitNodeOut(i int, timeout time.Duration) error {
	id := c.Nodes[i].ID()
	return c.WaitRing(func(r *ring.Ring) bool { _, in := r.Member(id); return !in }, timeout)
}

// KillMeta stops a meta node abruptly (no leadership transfer).
func (c *Cluster) KillMeta(i int) {
	c.mu.Lock()
	m, up := c.Metas[i], c.metaUp[i]
	c.metaUp[i] = false
	c.mu.Unlock()
	if up {
		m.Shutdown(false)
	}
}

// RestartMeta brings a killed meta node back on its old addresses with its
// old Raft state.
func (c *Cluster) RestartMeta(i int) error {
	p := c.Peers[i]
	m, err := c.openMeta(i, p.RaftAddr, p.GRPCAddr)
	if err != nil {
		return err
	}
	if err := m.Start(c.Peers, false); err != nil {
		return err
	}
	c.mu.Lock()
	c.Metas[i], c.metaUp[i] = m, true
	c.mu.Unlock()
	return nil
}

// AddNode starts a new storage node and returns its index.
func (c *Cluster) AddNode() (int, error) {
	c.mu.Lock()
	i := len(c.Nodes)
	c.mu.Unlock()
	n, err := c.openNode(i, "127.0.0.1:0")
	if err != nil {
		return -1, err
	}
	n.Start()
	c.mu.Lock()
	c.Nodes = append(c.Nodes, n)
	c.nodeUp = append(c.nodeUp, true)
	c.nodeAddr = append(c.nodeAddr, n.Addr())
	c.mu.Unlock()
	return i, nil
}

// KillNode crashes storage node i (its data stays on disk).
func (c *Cluster) KillNode(i int) {
	c.mu.Lock()
	n, up := c.Nodes[i], c.nodeUp[i]
	c.nodeUp[i] = false
	c.mu.Unlock()
	if up {
		n.Kill()
	}
}

// WipeNode deletes a (killed) node's data, simulating a lost disk.
func (c *Cluster) WipeNode(i int) error {
	return os.RemoveAll(filepath.Join(c.opts.Dir, fmt.Sprintf("node-%02d", i)))
}

// RestartNode starts node i again on its old address.
func (c *Cluster) RestartNode(i int) error {
	n, err := c.openNode(i, c.nodeAddr[i])
	if err != nil {
		return err
	}
	n.Start()
	c.mu.Lock()
	c.Nodes[i], c.nodeUp[i] = n, true
	c.mu.Unlock()
	return nil
}

func (c *Cluster) NodeUp(i int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodeUp[i]
}

// NodeByID returns the index of the storage node with that ID.
func (c *Cluster) NodeByID(id string) int {
	for i, n := range c.Nodes {
		if n.ID() == id {
			return i
		}
	}
	return -1
}

// Holders returns the indexes of live nodes holding chunk id.
func (c *Cluster) Holders(id string) []int {
	var out []int
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, n := range c.Nodes {
		if c.nodeUp[i] && n.Store().Has(id) {
			out = append(out, i)
		}
	}
	return out
}

// Repair runs a repair pass on the leader and waits for it.
func (c *Cluster) Repair(ctx context.Context) error {
	_, err := c.MetaClient.TriggerRepair(ctx, true)
	return err
}

func (c *Cluster) Metastore() metastore.Store { return c.opts.Metastore }
func (c *Cluster) Blob() blob.Store           { return c.opts.Blob }

func (c *Cluster) Close() {
	if c.Client != nil {
		c.Client.Close()
	}
	c.mu.Lock()
	nodes, metas := append([]*storage.Node(nil), c.Nodes...), append([]*meta.Server(nil), c.Metas...)
	c.mu.Unlock()
	var wg sync.WaitGroup
	for _, n := range nodes {
		wg.Add(1)
		go func(n *storage.Node) { defer wg.Done(); n.Kill() }(n)
	}
	wg.Wait()
	for _, m := range metas {
		m.Shutdown(false)
	}
	if c.MetaClient != nil {
		c.MetaClient.Close()
	}
}
