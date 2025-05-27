// Package coord implements file-level operations on top of the chunk plane.
// Any storage node can coordinate a request (there is no gateway tier):
//
// Upload splits the stream into fixed-size chunks, names each by its SHA-256,
// and writes it to the chunk's N preference-list replicas in parallel,
// returning once W of them have acknowledged (N=3, W=2 by default). If fewer
// than W preference replicas succeed it keeps walking the ring (a sloppy
// quorum) and the repair pass later moves the extra copies' data back into
// place. Only after every chunk reached quorum does it commit the manifest,
// so a failed upload leaves no visible file.
//
// Download reads chunks with bounded read-ahead, verifies every chunk's hash,
// and falls through to the next replica, then to a node-side fetch that can
// reach the S3 tier.
package coord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/cas"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

const DefaultChunkSize = 4 << 20

var (
	ErrNoRing       = errors.New("coord: no storage nodes in the ring yet")
	ErrQuorum       = errors.New("coord: write quorum not reached")
	ErrChunkMissing = errors.New("coord: no replica could serve the chunk")
)

// Local lets the coordinator short-circuit replicas that live on its own node.
type Local interface {
	PutLocal(id string, data []byte, backup bool) (alreadyPresent bool, err error)
	ReadLocal(id string) ([]byte, error)
}

type Config struct {
	Replicas    int // N
	WriteQuorum int // W
	ChunkSize   int
	// Parallelism is the number of chunks in flight per upload or download.
	Parallelism int
	Meta        metastore.Store
	Pool        *rpcutil.Pool
	Ring        func() *ring.Ring
	// Known returns every storage node the control plane knows, including
	// ones marked DOWN (which may in fact be alive). Used to locate chunks
	// that are not where the current ring says. Optional.
	Known      func() []ring.Member
	SelfID     string
	Local      Local
	RPCTimeout time.Duration
	Logger     *slog.Logger
}

type Coordinator struct {
	cfg  Config
	bufs sync.Pool

	// Counters, exported for tests and status.
	SloppyWrites atomic.Int64
	ReadFailover atomic.Int64
	ReadLocated  atomic.Int64 // reads served by a node found via Locate
}

func New(cfg Config) *Coordinator {
	if cfg.Replicas <= 0 {
		cfg.Replicas = 3
	}
	if cfg.WriteQuorum <= 0 {
		cfg.WriteQuorum = cfg.Replicas/2 + 1
	}
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = DefaultChunkSize
	}
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 4
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 30 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	c := &Coordinator{cfg: cfg}
	c.bufs.New = func() any { b := make([]byte, cfg.ChunkSize); return &b }
	return c
}

// sharedBuf is a pooled chunk buffer released when its last user is done.
type sharedBuf struct {
	b    *[]byte
	refs atomic.Int32
	pool *sync.Pool
}

func (s *sharedBuf) release() {
	if s.refs.Add(-1) == 0 {
		s.pool.Put(s.b)
	}
}

// Upload stores r as a new version of name.
func (c *Coordinator) Upload(ctx context.Context, name string, r io.Reader) (metastore.File, error) {
	if name == "" {
		return metastore.File{}, errors.New("coord: empty file name")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		fileHash = sha256.New()
		refs     []metastore.ChunkRef
		total    int64
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
		sem      = make(chan struct{}, c.cfg.Parallelism)
	)
	fail := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
			cancel()
		}
		errMu.Unlock()
	}

	for {
		bp := c.bufs.Get().(*[]byte)
		n, rerr := io.ReadFull(r, *bp)
		if n > 0 {
			data := (*bp)[:n]
			fileHash.Write(data)
			id := cas.ID(data)
			refs = append(refs, metastore.ChunkRef{ID: id, Size: int64(n)})
			total += int64(n)
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				c.bufs.Put(bp)
				wg.Wait()
				return metastore.File{}, c.uploadErr(ctx, firstErr)
			}
			sb := &sharedBuf{b: bp, pool: &c.bufs}
			sb.refs.Store(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				if err := c.writeChunk(ctx, id, data, sb); err != nil {
					fail(fmt.Errorf("chunk %s: %w", id[:12], err))
				}
				sb.release()
			}()
		} else {
			c.bufs.Put(bp)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			fail(rerr)
			break
		}
	}
	wg.Wait()
	if firstErr != nil {
		return metastore.File{}, firstErr
	}
	if err := c.cfg.Meta.PutChunks(ctx, refs); err != nil {
		return metastore.File{}, fmt.Errorf("coord: record chunks: %w", err)
	}
	f, err := c.cfg.Meta.CommitFile(ctx, metastore.File{
		Name: name, Size: total, SHA256: hex.EncodeToString(fileHash.Sum(nil)), Chunks: refs,
	})
	if err != nil {
		return metastore.File{}, fmt.Errorf("coord: commit manifest: %w", err)
	}
	return f, nil
}

func (c *Coordinator) uploadErr(ctx context.Context, first error) error {
	if first != nil {
		return first
	}
	return ctx.Err()
}

// writeChunk writes one chunk to a write quorum.
func (c *Coordinator) writeChunk(ctx context.Context, id string, data []byte, sb *sharedBuf) error {
	r := c.cfg.Ring()
	if r == nil || r.Len() == 0 {
		return ErrNoRing
	}
	cands := r.Lookup(id, r.Len())
	n := min(c.cfg.Replicas, len(cands))
	// W is not reduced when the ring shrinks: acknowledging a write with
	// fewer copies than W would silently lower durability.
	w := c.cfg.WriteQuorum
	if len(cands) < w {
		return fmt.Errorf("%w: ring has %d nodes, write quorum is %d", ErrQuorum, len(cands), w)
	}
	prefs, rest := cands[:n], cands[n:]

	type res struct {
		idx int
		err error
	}
	results := make(chan res, n)
	for i, m := range prefs {
		sb.refs.Add(1)
		go func(i int, m ring.Member) {
			defer sb.release()
			// Replicas keep writing after the quorum returns, so they get
			// their own deadline rather than the request context.
			pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.RPCTimeout)
			defer cancel()
			results <- res{i, c.putReplica(pctx, m, id, data, i == 0)}
		}(i, m)
	}
	ok, done := 0, 0
	primaryOK := false
	var errs []error
	for done < n {
		select {
		case rr := <-results:
			done++
			if rr.err == nil {
				ok++
				if rr.idx == 0 {
					primaryOK = true
				}
			} else {
				errs = append(errs, fmt.Errorf("%s: %w", prefs[rr.idx].ID, rr.err))
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		if ok >= w {
			return nil
		}
		if ok+(n-done) < w {
			break // quorum impossible from the preference list
		}
	}
	// Sloppy quorum: continue clockwise past the preference list.
	for _, m := range rest {
		if ok >= w {
			break
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
		err := c.putReplica(pctx, m, id, data, !primaryOK)
		cancel()
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", m.ID, err))
			continue
		}
		ok++
		primaryOK = true
		c.SloppyWrites.Add(1)
	}
	if ok >= w {
		return nil
	}
	return fmt.Errorf("%w (%d/%d): %w", ErrQuorum, ok, w, errors.Join(errs...))
}

func (c *Coordinator) putReplica(ctx context.Context, m ring.Member, id string, data []byte, backup bool) error {
	if c.cfg.Local != nil && m.ID == c.cfg.SelfID {
		_, err := c.cfg.Local.PutLocal(id, data, backup)
		return err
	}
	conn, err := c.cfg.Pool.Get(m.Addr)
	if err != nil {
		return err
	}
	return PutChunk(ctx, dfsv1.NewChunkServiceClient(conn), id, data, backup)
}

// PutChunk streams one chunk to a ChunkService.
func PutChunk(ctx context.Context, cl dfsv1.ChunkServiceClient, id string, data []byte, backup bool) error {
	st, err := cl.PutChunk(ctx)
	if err != nil {
		return err
	}
	if err := st.Send(&dfsv1.PutChunkRequest{Msg: &dfsv1.PutChunkRequest_Header{Header: &dfsv1.PutChunkHeader{
		ChunkId: id, Size: int64(len(data)), Backup: backup,
	}}}); err != nil {
		return streamErr(st.CloseAndRecv, err)
	}
	for off := 0; off < len(data); off += rpcutil.FrameSize {
		end := min(off+rpcutil.FrameSize, len(data))
		if err := st.Send(&dfsv1.PutChunkRequest{Msg: &dfsv1.PutChunkRequest_Data{Data: data[off:end]}}); err != nil {
			return streamErr(st.CloseAndRecv, err)
		}
	}
	_, err = st.CloseAndRecv()
	return err
}

// streamErr surfaces the server's status when Send fails with io.EOF.
func streamErr(closeAndRecv func() (*dfsv1.PutChunkResponse, error), err error) error {
	if errors.Is(err, io.EOF) {
		if _, rerr := closeAndRecv(); rerr != nil {
			return rerr
		}
	}
	return err
}

// GetChunk reads one chunk from a ChunkService and checks its size and hash.
func GetChunk(ctx context.Context, cl dfsv1.ChunkServiceClient, id string, fallback bool) ([]byte, error) {
	st, err := cl.GetChunk(ctx, &dfsv1.GetChunkRequest{ChunkId: id, Fallback: fallback})
	if err != nil {
		return nil, err
	}
	var buf []byte
	first := true
	for {
		msg, err := st.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if first {
			buf = make([]byte, 0, msg.Size)
			first = false
		}
		buf = append(buf, msg.Data...)
	}
	if cas.ID(buf) != id {
		return nil, cas.ErrHashMismatch
	}
	return buf, nil
}

// Download writes the content of f to w.
func (c *Coordinator) Download(ctx context.Context, f metastore.File, w io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	type result struct {
		data []byte
		err  error
	}
	slots := make([]chan result, len(f.Chunks))
	for i := range slots {
		slots[i] = make(chan result, 1)
	}
	sem := make(chan struct{}, c.cfg.Parallelism)
	go func() {
		for i, ch := range f.Chunks {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(i int, ch metastore.ChunkRef) {
				data, err := c.ReadChunk(ctx, ch.ID)
				if err == nil && int64(len(data)) != ch.Size {
					err = fmt.Errorf("coord: chunk %s size %d, manifest says %d", ch.ID[:12], len(data), ch.Size)
				}
				slots[i] <- result{data, err}
			}(i, ch)
		}
	}()
	h := sha256.New()
	for i := range f.Chunks {
		var r result
		select {
		case r = <-slots[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
		<-sem
		if r.err != nil {
			return fmt.Errorf("coord: chunk %d/%d: %w", i+1, len(f.Chunks), r.err)
		}
		h.Write(r.data)
		if _, err := w.Write(r.data); err != nil {
			return err
		}
	}
	if got := hex.EncodeToString(h.Sum(nil)); f.SHA256 != "" && got != f.SHA256 {
		return fmt.Errorf("coord: file checksum mismatch: manifest %s, got %s", f.SHA256, got)
	}
	return nil
}

// ReadChunk fetches one chunk, trying the local store, then each replica in
// preference order, then the next nodes on the ring (sloppy-quorum copies),
// and finally asking the primary to fetch it on our behalf (which can reach
// peers the coordinator could not, and the S3 tier).
func (c *Coordinator) ReadChunk(ctx context.Context, id string) ([]byte, error) {
	r := c.cfg.Ring()
	if r == nil || r.Len() == 0 {
		return nil, ErrNoRing
	}
	n := min(c.cfg.Replicas, r.Len())
	cands := r.Lookup(id, min(n+2, r.Len()))
	if c.cfg.Local != nil {
		for _, m := range cands[:n] {
			if m.ID == c.cfg.SelfID {
				if b, err := c.cfg.Local.ReadLocal(id); err == nil {
					return b, nil
				}
			}
		}
	}
	var errs []error
	for i, m := range cands {
		if m.ID == c.cfg.SelfID {
			continue
		}
		b, err := c.getRemote(ctx, m.Addr, id, false)
		if err == nil {
			if i > 0 {
				c.ReadFailover.Add(1)
			}
			return b, nil
		}
		errs = append(errs, err)
	}
	// The chunk is not where the current ring puts it. That happens after
	// membership churn (nodes added, or marked DOWN and back) until the next
	// repair pass moves replicas into place. Ask every known node.
	tried := map[string]bool{}
	for _, m := range cands {
		tried[m.Addr] = true
	}
	known := r.Members()
	if c.cfg.Known != nil {
		known = c.cfg.Known()
	}
	var others []ring.Member
	for _, m := range known {
		if !tried[m.Addr] && m.ID != c.cfg.SelfID {
			others = append(others, m)
		}
	}
	for _, addr := range Locate(ctx, c.cfg.Pool, others, id) {
		b, err := c.getRemote(ctx, addr, id, false)
		if err == nil {
			c.ReadFailover.Add(1)
			c.ReadLocated.Add(1)
			return b, nil
		}
		errs = append(errs, err)
	}
	// Last resort: have a preference node fetch it (peers, then S3).
	for _, m := range cands[:n] {
		if m.ID == c.cfg.SelfID {
			continue
		}
		b, err := c.getRemote(ctx, m.Addr, id, true)
		if err == nil {
			c.ReadFailover.Add(1)
			return b, nil
		}
		errs = append(errs, err)
	}
	return nil, fmt.Errorf("%w %s: %w", ErrChunkMissing, id[:12], errors.Join(errs...))
}

// Locate asks the given nodes, in parallel, whether they hold chunk id and
// returns the addresses of those that do.
func Locate(ctx context.Context, pool *rpcutil.Pool, nodes []ring.Member, id string) []string {
	if len(nodes) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var mu sync.Mutex
	var out []string
	var wg sync.WaitGroup
	for _, m := range nodes {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			conn, err := pool.Get(addr)
			if err != nil {
				return
			}
			resp, err := dfsv1.NewChunkServiceClient(conn).HasChunks(ctx, &dfsv1.HasChunksRequest{ChunkIds: []string{id}})
			if err == nil && len(resp.Present) == 1 && resp.Present[0] {
				mu.Lock()
				out = append(out, addr)
				mu.Unlock()
			}
		}(m.Addr)
	}
	wg.Wait()
	return out
}

func (c *Coordinator) getRemote(ctx context.Context, addr, id string, fallback bool) ([]byte, error) {
	conn, err := c.cfg.Pool.Get(addr)
	if err != nil {
		return nil, err
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	return GetChunk(cctx, dfsv1.NewChunkServiceClient(conn), id, fallback)
}
