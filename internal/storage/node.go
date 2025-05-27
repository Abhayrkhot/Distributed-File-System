// Package storage is a storage node: it owns a local chunk store, serves the
// ChunkService (data plane) and the FileService (any node coordinates client
// requests), heartbeats to the Raft leader, follows the ring, and copies
// chunks to the S3 tier in the background.
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/cas"
	"github.com/Abhayrkhot/Distributed-File-System/internal/coord"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

type Config struct {
	ID        string
	Bind      string
	Advertise string
	DataDir   string
	Fsync     bool

	MetaAddrs []string
	// Metastore backs the FileService. Nil disables it on this node.
	Metastore metastore.Store
	// Blob is the S3 tier. Nil disables backups and S3 fallback reads.
	Blob blob.Store

	BackupWorkers int
	BackupQueue   int

	HeartbeatInterval time.Duration
	Replicas          int
	WriteQuorum       int
	ChunkSize         int
	Parallelism       int

	Logger *slog.Logger
}

type Node struct {
	cfg   Config
	log   *slog.Logger
	store *cas.Store
	pool  *rpcutil.Pool
	meta  *meta.Client
	coord *coord.Coordinator

	ring  atomic.Pointer[ring.Ring]
	known atomic.Pointer[[]ring.Member] // all registered nodes, UP or DOWN

	lis     net.Listener
	srv     *grpc.Server
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	stopped atomic.Bool

	backupQ        chan string
	backupInflight sync.Map
	backupsDone    atomic.Int64
	backupsFailed  atomic.Int64
	backupsDropped atomic.Int64
	backupsActive  atomic.Int64

	dfsv1.UnimplementedChunkServiceServer
}

// Open binds the listener and opens the chunk store.
func Open(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("storage: empty node id")
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 250 * time.Millisecond
	}
	if cfg.BackupWorkers == 0 {
		cfg.BackupWorkers = 4
	}
	if cfg.BackupQueue == 0 {
		cfg.BackupQueue = 8192
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	st, err := cas.Open(cas.Options{Root: cfg.DataDir, Fsync: cfg.Fsync})
	if err != nil {
		return nil, err
	}
	lis, err := rpcutil.Listen(cfg.Bind)
	if err != nil {
		return nil, err
	}
	n := &Node{
		cfg:     cfg,
		log:     cfg.Logger.With("node", cfg.ID),
		store:   st,
		pool:    rpcutil.NewPool(),
		lis:     lis,
		backupQ: make(chan string, cfg.BackupQueue),
	}
	n.ring.Store(ring.New(0, nil, ring.DefaultVnodes))
	n.meta = meta.NewClient(cfg.MetaAddrs, n.pool)
	n.coord = coord.New(coord.Config{
		Replicas: cfg.Replicas, WriteQuorum: cfg.WriteQuorum, ChunkSize: cfg.ChunkSize, Parallelism: cfg.Parallelism,
		Meta: cfg.Metastore, Pool: n.pool, Ring: n.Ring, Known: n.Known, SelfID: cfg.ID, Local: n, Logger: n.log,
	})
	return n, nil
}

func (n *Node) ID() string { return n.cfg.ID }

// Addr is the address other nodes and clients dial.
func (n *Node) Addr() string {
	if n.cfg.Advertise != "" {
		return n.cfg.Advertise
	}
	return n.lis.Addr().String()
}

// Known returns every storage node registered with the control plane.
func (n *Node) Known() []ring.Member {
	if k := n.known.Load(); k != nil {
		return *k
	}
	return n.Ring().Members()
}

func (n *Node) Ring() *ring.Ring                { return n.ring.Load() }
func (n *Node) Store() *cas.Store               { return n.store }
func (n *Node) Coordinator() *coord.Coordinator { return n.coord }

// Start serves gRPC and starts the background loops.
func (n *Node) Start() {
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.srv = grpc.NewServer(rpcutil.ServerOptions()...)
	dfsv1.RegisterChunkServiceServer(n.srv, n)
	if n.cfg.Metastore != nil {
		dfsv1.RegisterFileServiceServer(n.srv, &fileServer{n: n})
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.srv.Serve(n.lis) }()

	n.wg.Add(2)
	go func() { defer n.wg.Done(); n.heartbeatLoop() }()
	go func() {
		defer n.wg.Done()
		n.meta.WatchRing(n.ctx, func(r *ring.Ring, rp *dfsv1.Ring) {
			all := make([]ring.Member, 0, len(rp.Nodes))
			for _, sn := range rp.Nodes {
				all = append(all, ring.Member{ID: sn.Id, Addr: sn.Addr})
			}
			n.known.Store(&all)
			if old := n.ring.Load(); old == nil || r.Epoch() >= old.Epoch() {
				n.ring.Store(r)
			}
		})
	}()
	if n.cfg.Blob != nil {
		for i := 0; i < n.cfg.BackupWorkers; i++ {
			n.wg.Add(1)
			go func() { defer n.wg.Done(); n.backupWorker() }()
		}
	}
}

// Kill simulates a crash: the server stops immediately, in-flight streams
// fail, and heartbeats stop. Data on disk is kept.
func (n *Node) Kill() {
	if n.stopped.Swap(true) {
		return
	}
	n.cancel()
	n.srv.Stop()
	n.wg.Wait()
	n.pool.Close()
}

// Shutdown drains in-flight requests first.
func (n *Node) Shutdown() {
	if n.stopped.Swap(true) {
		return
	}
	done := make(chan struct{})
	go func() { n.srv.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		n.srv.Stop()
	}
	n.cancel()
	n.wg.Wait()
	n.pool.Close()
}

func (n *Node) heartbeatLoop() {
	t := time.NewTicker(n.cfg.HeartbeatInterval)
	defer t.Stop()
	var lastErr time.Time
	for {
		chunks, bytes := n.store.Stats()
		ctx, cancel := context.WithTimeout(n.ctx, 2*time.Second)
		_, err := n.meta.Heartbeat(ctx, &dfsv1.HeartbeatRequest{
			NodeId: n.cfg.ID, Addr: n.Addr(), Chunks: chunks, Bytes: bytes, RingEpoch: n.Ring().Epoch(),
		})
		cancel()
		if err != nil && n.ctx.Err() == nil && time.Since(lastErr) > 5*time.Second {
			n.log.Warn("heartbeat failed", "err", err)
			lastErr = time.Now()
		}
		select {
		case <-n.ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ---------------------------------------------------------------------------
// coord.Local

func (n *Node) PutLocal(id string, data []byte, backup bool) (bool, error) {
	already, err := n.store.PutBytes(id, data)
	if err == nil && backup {
		n.enqueueBackup(id)
	}
	return already, err
}

func (n *Node) ReadLocal(id string) ([]byte, error) { return n.store.ReadAll(id) }

// ---------------------------------------------------------------------------
// S3 write-behind

func (n *Node) enqueueBackup(id string) {
	if n.cfg.Blob == nil {
		return
	}
	select {
	case n.backupQ <- id:
	default:
		// The repair pass finds chunks without an S3 copy and re-requests them.
		n.backupsDropped.Add(1)
	}
}

func (n *Node) backupWorker() {
	for {
		select {
		case <-n.ctx.Done():
			return
		case id := <-n.backupQ:
			n.backupsActive.Add(1)
			err := n.backup(n.ctx, id, false)
			n.backupsActive.Add(-1)
			if err != nil && n.ctx.Err() == nil {
				n.backupsFailed.Add(1)
				n.log.Warn("s3 backup failed", "chunk", id[:12], "err", err)
			}
		}
	}
}

// backup uploads a local chunk to S3. With check, it skips chunks already
// there. Concurrent requests for the same chunk collapse into one upload.
func (n *Node) backup(ctx context.Context, id string, check bool) error {
	if _, busy := n.backupInflight.LoadOrStore(id, struct{}{}); busy {
		return nil
	}
	defer n.backupInflight.Delete(id)
	key := blob.ChunkKey(id)
	if check {
		if ok, err := n.cfg.Blob.Exists(ctx, key); err == nil && ok {
			return nil
		}
	}
	data, err := n.store.ReadAll(id)
	if err != nil {
		return err
	}
	if err := n.cfg.Blob.Put(ctx, key, data); err != nil {
		return err
	}
	n.backupsDone.Add(1)
	return nil
}

// ---------------------------------------------------------------------------
// ChunkService

type streamReader struct {
	recv func() ([]byte, error)
	buf  []byte
}

func (r *streamReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		b, err := r.recv()
		if err != nil {
			return 0, err
		}
		r.buf = b
	}
	k := copy(p, r.buf)
	r.buf = r.buf[k:]
	return k, nil
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cas.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, cas.ErrInvalidID), errors.Is(err, cas.ErrHashMismatch), errors.Is(err, cas.ErrSizeMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Internal, err.Error())
}

func (n *Node) PutChunk(stream dfsv1.ChunkService_PutChunkServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := first.GetHeader()
	if h == nil {
		return status.Error(codes.InvalidArgument, "first message must be a header")
	}
	r := &streamReader{recv: func() ([]byte, error) {
		m, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		return m.GetData(), nil
	}}
	written, already, err := n.store.Put(h.ChunkId, h.Size, r)
	if err != nil {
		return toStatus(err)
	}
	if h.Backup {
		n.enqueueBackup(h.ChunkId)
	}
	return stream.SendAndClose(&dfsv1.PutChunkResponse{AlreadyPresent: already, BytesWritten: written})
}

func (n *Node) GetChunk(req *dfsv1.GetChunkRequest, stream dfsv1.ChunkService_GetChunkServer) error {
	rc, size, err := n.store.Open(req.ChunkId)
	if errors.Is(err, cas.ErrNotFound) && req.Fallback {
		if _, ferr := n.fetch(stream.Context(), req.ChunkId, nil, true); ferr != nil {
			return status.Errorf(codes.NotFound, "chunk %s not local and fallback failed: %v", req.ChunkId, ferr)
		}
		rc, size, err = n.store.Open(req.ChunkId)
	}
	if err != nil {
		return toStatus(err)
	}
	defer rc.Close()
	buf := make([]byte, rpcutil.FrameSize)
	first := true
	for {
		k, rerr := io.ReadFull(rc, buf)
		if k > 0 || first {
			msg := &dfsv1.GetChunkResponse{Data: buf[:k]}
			if first {
				msg.Size = size
				first = false
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			return nil
		}
		if rerr != nil {
			return toStatus(rerr)
		}
	}
}

func (n *Node) HasChunks(ctx context.Context, req *dfsv1.HasChunksRequest) (*dfsv1.HasChunksResponse, error) {
	out := make([]bool, len(req.ChunkIds))
	for i, id := range req.ChunkIds {
		out[i] = n.store.Has(id)
	}
	return &dfsv1.HasChunksResponse{Present: out}, nil
}

// fetch pulls a chunk into the local store from the given sources (or, when
// sources is nil, from the chunk's other replicas on the current ring), then
// from S3 if allowed. It returns where the chunk came from.
func (n *Node) fetch(ctx context.Context, id string, sources []string, allowS3 bool) (string, error) {
	if n.store.Has(id) {
		return "local", nil
	}
	if sources == nil {
		for _, m := range n.Ring().Lookup(id, n.Ring().Len()) {
			if m.ID != n.cfg.ID {
				sources = append(sources, m.Addr)
			}
			if len(sources) >= 5 {
				break
			}
		}
	}
	var errs []error
	for _, addr := range sources {
		if addr == n.Addr() {
			continue
		}
		conn, err := n.pool.Get(addr)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		data, err := coord.GetChunk(cctx, dfsv1.NewChunkServiceClient(conn), id, false)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", addr, err))
			continue
		}
		if _, err := n.store.PutBytes(id, data); err != nil {
			return "", err
		}
		return "peer:" + addr, nil
	}
	if located := n.locateOthers(ctx, id, sources); len(located) > 0 {
		for _, addr := range located {
			conn, err := n.pool.Get(addr)
			if err != nil {
				continue
			}
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			data, err := coord.GetChunk(cctx, dfsv1.NewChunkServiceClient(conn), id, false)
			cancel()
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", addr, err))
				continue
			}
			if _, err := n.store.PutBytes(id, data); err != nil {
				return "", err
			}
			return "peer:" + addr, nil
		}
	}
	if allowS3 && n.cfg.Blob != nil {
		data, err := n.cfg.Blob.Get(ctx, blob.ChunkKey(id))
		if err != nil {
			errs = append(errs, fmt.Errorf("s3: %w", err))
		} else {
			if cas.ID(data) != id {
				return "", fmt.Errorf("s3 copy of %s is corrupt", id[:12])
			}
			if _, err := n.store.PutBytes(id, data); err != nil {
				return "", err
			}
			return "s3", nil
		}
	}
	return "", errors.Join(append(errs, fmt.Errorf("no source could serve %s", id[:12]))...)
}

// locateOthers finds holders of id among known nodes not already tried.
func (n *Node) locateOthers(ctx context.Context, id string, tried []string) []string {
	skip := map[string]bool{n.Addr(): true}
	for _, a := range tried {
		skip[a] = true
	}
	var others []ring.Member
	for _, m := range n.Known() {
		if !skip[m.Addr] {
			others = append(others, m)
		}
	}
	return coord.Locate(ctx, n.pool, others, id)
}

func (n *Node) ReplicateChunk(ctx context.Context, req *dfsv1.ReplicateChunkRequest) (*dfsv1.ReplicateChunkResponse, error) {
	if !cas.ValidID(req.ChunkId) {
		return nil, status.Error(codes.InvalidArgument, "invalid chunk id")
	}
	src, err := n.fetch(ctx, req.ChunkId, append([]string{}, req.Sources...), req.AllowS3)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	sz, _ := n.store.Stat(req.ChunkId)
	return &dfsv1.ReplicateChunkResponse{Source: src, Bytes: sz}, nil
}

func (n *Node) BackupChunk(ctx context.Context, req *dfsv1.BackupChunkRequest) (*dfsv1.BackupChunkResponse, error) {
	if n.cfg.Blob == nil {
		return nil, status.Error(codes.FailedPrecondition, "no S3 tier configured")
	}
	if !n.store.Has(req.ChunkId) {
		return nil, status.Error(codes.NotFound, "chunk not held by this node")
	}
	if err := n.backup(ctx, req.ChunkId, true); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return &dfsv1.BackupChunkResponse{}, nil
}

func (n *Node) NodeStatus(ctx context.Context, _ *dfsv1.NodeStatusRequest) (*dfsv1.NodeStatusResponse, error) {
	chunks, bytes := n.store.Stats()
	return &dfsv1.NodeStatusResponse{
		NodeId: n.cfg.ID, Addr: n.Addr(), Chunks: chunks, Bytes: bytes, RingEpoch: n.Ring().Epoch(),
		BackupQueueDepth: int64(len(n.backupQ)), BackupsDone: n.backupsDone.Load(), BackupsFailed: n.backupsFailed.Load() + n.backupsDropped.Load(),
		BackupsInflight: n.backupsActive.Load(),
	}, nil
}
