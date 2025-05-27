// Package repair restores the replication factor. The Raft leader runs it
// after every membership epoch change (debounced) and on a timer.
//
// One pass: page through the chunk catalogue; compute each chunk's preference
// list on the current ring; ask each storage node, in one batched HasChunks
// call per node per page, which of its assigned chunks it holds; then tell
// every node that is missing a chunk to pull it from a surviving replica, or
// from the S3 tier when no replica survives. It also checks that every chunk
// has an S3 copy and asks a holder to upload any that are missing (this
// covers write-behind uploads that were dropped or still queued when the
// primary died).
package repair

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

type Config struct {
	Replicas    int
	Meta        metastore.Store
	Blob        blob.Store // nil disables S3 checks and S3 restores
	Pool        *rpcutil.Pool
	Concurrency int
	RPCTimeout  time.Duration
	Logger      *slog.Logger
}

// Result summarises one pass.
type Result struct {
	Epoch            uint64
	Duration         time.Duration
	ChunksScanned    int64
	UnderReplicated  int64
	ReplicasRepaired int64
	RestoredFromS3   int64
	BackupsTriggered int64
	Unrecoverable    int64
	Errors           int64
}

type Coordinator struct {
	cfg Config

	mu      sync.Mutex
	running bool
	runs    int64
	last    Result
	lastAt  time.Time
}

func New(cfg Config) *Coordinator {
	if cfg.Replicas <= 0 {
		cfg.Replicas = 3
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Coordinator{cfg: cfg}
}

// Status reports the last completed pass.
func (c *Coordinator) Status() *dfsv1.RepairStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &dfsv1.RepairStatus{
		Running:           c.running,
		Runs:              c.runs,
		LastStartedUnixMs: c.lastAt.UnixMilli(),
		LastDurationMs:    c.last.Duration.Milliseconds(),
		LastEpoch:         c.last.Epoch,
		ChunksScanned:     c.last.ChunksScanned,
		ReplicasRepaired:  c.last.ReplicasRepaired,
		RestoredFromS3:    c.last.RestoredFromS3,
		BackupsTriggered:  c.last.BackupsTriggered,
		UnderReplicated:   c.last.UnderReplicated,
		Errors:            c.last.Errors + c.last.Unrecoverable,
	}
}

type task struct {
	chunk   metastore.ChunkRef
	target  ring.Member
	sources []string
	allowS3 bool
}

// Run performs one repair pass against r. Passes are serialised by the caller.
func (c *Coordinator) Run(ctx context.Context, r *ring.Ring) (Result, error) {
	start := time.Now()
	c.mu.Lock()
	c.running = true
	c.lastAt = start
	c.mu.Unlock()

	res := Result{Epoch: r.Epoch()}
	var err error
	defer func() {
		res.Duration = time.Since(start)
		c.mu.Lock()
		c.running = false
		c.runs++
		c.last = res
		c.mu.Unlock()
		lvl := slog.LevelInfo
		if res.UnderReplicated == 0 && res.BackupsTriggered == 0 {
			lvl = slog.LevelDebug
		}
		c.cfg.Logger.Log(ctx, lvl, "repair pass done", "epoch", res.Epoch, "dur", res.Duration.Round(time.Millisecond),
			"scanned", res.ChunksScanned, "under_replicated", res.UnderReplicated, "repaired", res.ReplicasRepaired,
			"from_s3", res.RestoredFromS3, "backups", res.BackupsTriggered, "unrecoverable", res.Unrecoverable,
			"errors", res.Errors, "err", err)
	}()

	if r.Len() == 0 {
		return res, nil
	}
	var backedUp map[string]struct{}
	if c.cfg.Blob != nil {
		backedUp = map[string]struct{}{}
		if lerr := c.cfg.Blob.List(ctx, blob.ChunkPrefix, func(key string) error {
			if id, ok := blob.ChunkIDFromKey(key); ok {
				backedUp[id] = struct{}{}
			}
			return nil
		}); lerr != nil {
			// Without the listing, don't trigger backups or S3 restores.
			c.cfg.Logger.Warn("repair: listing S3 tier failed", "err", lerr)
			backedUp = nil
			res.Errors++
		}
	}

	err = c.cfg.Meta.ScanChunks(ctx, func(page []metastore.ChunkRef) error {
		return c.repairPage(ctx, r, page, backedUp, &res)
	})
	return res, err
}

func (c *Coordinator) repairPage(ctx context.Context, r *ring.Ring, page []metastore.ChunkRef, backedUp map[string]struct{}, res *Result) error {
	n := c.cfg.Replicas
	prefs := make([][]ring.Member, len(page))
	byNode := map[string][]int{} // node ID -> indexes into page
	for i, ch := range page {
		prefs[i] = r.Lookup(ch.ID, n)
		for _, m := range prefs[i] {
			byNode[m.ID] = append(byNode[m.ID], i)
		}
	}
	res.ChunksScanned += int64(len(page))

	present, unknown := c.probe(ctx, r, page, byNode)

	var tasks []task
	var backups []backupTask
	var orphans []int // chunks with no holder in their preference list
	for i, ch := range page {
		var holders []string
		var missing []ring.Member
		for _, m := range prefs[i] {
			switch {
			case unknown[m.ID]:
				// Can't tell; leave it for the next pass.
			case present[key(m.ID, i)]:
				holders = append(holders, m.Addr)
			default:
				missing = append(missing, m)
			}
		}
		_, inS3 := backedUp[ch.ID]
		if backedUp != nil && !inS3 && len(holders) > 0 {
			backups = append(backups, backupTask{chunk: ch, holders: holders})
		}
		if len(missing) == 0 {
			continue
		}
		res.UnderReplicated++
		if len(holders) == 0 {
			orphans = append(orphans, i)
		}
		for _, m := range missing {
			tasks = append(tasks, task{chunk: ch, target: m, sources: holders, allowS3: inS3})
		}
	}

	// A chunk with no holder in its preference list may still survive on a
	// node outside it (e.g. a node that came back after its ranges moved).
	// Ask every node about these few chunks before falling back to S3.
	if len(orphans) > 0 {
		extra := c.findAnywhere(ctx, r, page, orphans)
		for ti := range tasks {
			if srcs, ok := extra[tasks[ti].chunk.ID]; ok && len(tasks[ti].sources) == 0 {
				tasks[ti].sources = srcs
			}
		}
	}

	c.execute(ctx, tasks, res)
	c.backup(ctx, backups, res)
	return ctx.Err()
}

func key(node string, i int) string { return fmt.Sprintf("%s/%d", node, i) }

// probe asks each node which of its assigned chunks it holds.
func (c *Coordinator) probe(ctx context.Context, r *ring.Ring, page []metastore.ChunkRef, byNode map[string][]int) (map[string]bool, map[string]bool) {
	var mu sync.Mutex
	present := map[string]bool{}
	unknown := map[string]bool{}
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.Concurrency)
	for nodeID, idxs := range byNode {
		m, _ := r.Member(nodeID)
		wg.Add(1)
		sem <- struct{}{}
		go func(m ring.Member, idxs []int) {
			defer wg.Done()
			defer func() { <-sem }()
			ids := make([]string, len(idxs))
			for j, i := range idxs {
				ids[j] = page[i].ID
			}
			got, err := c.hasChunks(ctx, m.Addr, ids)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				unknown[m.ID] = true
				return
			}
			for j, i := range idxs {
				if got[j] {
					present[key(m.ID, i)] = true
				}
			}
		}(m, idxs)
	}
	wg.Wait()
	return present, unknown
}

func (c *Coordinator) hasChunks(ctx context.Context, addr string, ids []string) ([]bool, error) {
	conn, err := c.cfg.Pool.Get(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	resp, err := dfsv1.NewChunkServiceClient(conn).HasChunks(cctx, &dfsv1.HasChunksRequest{ChunkIds: ids})
	if err != nil {
		return nil, err
	}
	if len(resp.Present) != len(ids) {
		return nil, errors.New("repair: short HasChunks response")
	}
	return resp.Present, nil
}

func (c *Coordinator) findAnywhere(ctx context.Context, r *ring.Ring, page []metastore.ChunkRef, orphans []int) map[string][]string {
	ids := make([]string, len(orphans))
	for j, i := range orphans {
		ids[j] = page[i].ID
	}
	out := map[string][]string{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, m := range r.Members() {
		wg.Add(1)
		go func(m ring.Member) {
			defer wg.Done()
			got, err := c.hasChunks(ctx, m.Addr, ids)
			if err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for j, ok := range got {
				if ok {
					out[ids[j]] = append(out[ids[j]], m.Addr)
				}
			}
		}(m)
	}
	wg.Wait()
	return out
}

type backupTask struct {
	chunk   metastore.ChunkRef
	holders []string
}

// backup asks a holder of each chunk to upload it to the S3 tier.
func (c *Coordinator) backup(ctx context.Context, tasks []backupTask, res *Result) {
	var done, errs atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.Concurrency)
	for _, t := range tasks {
		wg.Add(1)
		sem <- struct{}{}
		go func(t backupTask) {
			defer wg.Done()
			defer func() { <-sem }()
			for _, addr := range t.holders {
				conn, err := c.cfg.Pool.Get(addr)
				if err != nil {
					continue
				}
				cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
				_, err = dfsv1.NewChunkServiceClient(conn).BackupChunk(cctx, &dfsv1.BackupChunkRequest{ChunkId: t.chunk.ID})
				cancel()
				if err == nil {
					done.Add(1)
					return
				}
			}
			errs.Add(1)
		}(t)
	}
	wg.Wait()
	res.BackupsTriggered += done.Load()
	res.Errors += errs.Load()
}

func (c *Coordinator) execute(ctx context.Context, tasks []task, res *Result) {
	var repaired, fromS3, unrecoverable, errs atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.cfg.Concurrency)
	for _, t := range tasks {
		if len(t.sources) == 0 && !t.allowS3 {
			unrecoverable.Add(1)
			c.cfg.Logger.Error("repair: chunk has no surviving replica and no S3 copy", "chunk", t.chunk.ID)
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(t task) {
			defer wg.Done()
			defer func() { <-sem }()
			conn, err := c.cfg.Pool.Get(t.target.Addr)
			if err != nil {
				errs.Add(1)
				return
			}
			cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
			defer cancel()
			resp, err := dfsv1.NewChunkServiceClient(conn).ReplicateChunk(cctx, &dfsv1.ReplicateChunkRequest{
				ChunkId: t.chunk.ID, Size: t.chunk.Size, Sources: t.sources, AllowS3: t.allowS3,
			})
			if err != nil {
				errs.Add(1)
				c.cfg.Logger.Warn("repair: replicate failed", "chunk", t.chunk.ID[:12], "target", t.target.ID, "err", err)
				return
			}
			repaired.Add(1)
			if resp.Source == "s3" {
				fromS3.Add(1)
			}
		}(t)
	}
	wg.Wait()
	res.ReplicasRepaired += repaired.Load()
	res.RestoredFromS3 += fromS3.Load()
	res.Unrecoverable += unrecoverable.Load()
	res.Errors += errs.Load()
}
