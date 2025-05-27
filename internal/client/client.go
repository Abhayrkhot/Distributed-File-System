// Package client is a thin FileService client used by the CLI, the
// benchmarks and the integration tests. It spreads requests across storage
// nodes (any node can coordinate) and retries on another node when the one it
// picked is unreachable.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"sync"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

type Client struct {
	pool *rpcutil.Pool
	mu   sync.RWMutex
	// Nodes are FileService addresses (storage nodes).
	nodes []string
}

func New(nodes []string) *Client {
	return &Client{pool: rpcutil.NewPool(), nodes: append([]string(nil), nodes...)}
}

func (c *Client) Close() { c.pool.Close() }

// SetNodes replaces the node list (e.g. from a ring refresh).
func (c *Client) SetNodes(nodes []string) {
	c.mu.Lock()
	c.nodes = append([]string(nil), nodes...)
	c.mu.Unlock()
}

func (c *Client) order() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := append([]string(nil), c.nodes...)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// Pick returns a random node to coordinate a request.
func (c *Client) Pick() string { return c.order()[0] }

func (c *Client) stub(addr string) (dfsv1.FileServiceClient, error) {
	conn, err := c.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return dfsv1.NewFileServiceClient(conn), nil
}

// UploadBytes uploads an in-memory file, retrying on another coordinator if
// the chosen one is unavailable.
func (c *Client) UploadBytes(ctx context.Context, name string, data []byte) (*dfsv1.FileInfo, error) {
	var errs []error
	for _, addr := range c.order() {
		fi, err := c.UploadTo(ctx, addr, name, &sliceReader{b: data})
		if err == nil {
			return fi, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
		if !rpcutil.IsUnavailable(err) || ctx.Err() != nil {
			break
		}
	}
	return nil, errors.Join(errs...)
}

// UploadTo streams r to the coordinator at addr.
func (c *Client) UploadTo(ctx context.Context, addr, name string, r io.Reader) (*dfsv1.FileInfo, error) {
	s, err := c.stub(addr)
	if err != nil {
		return nil, err
	}
	st, err := s.Upload(ctx)
	if err != nil {
		return nil, err
	}
	if err := st.Send(&dfsv1.UploadRequest{Msg: &dfsv1.UploadRequest_Header{Header: &dfsv1.UploadHeader{Name: name}}}); err != nil {
		return nil, closeErr(st, err)
	}
	buf := make([]byte, rpcutil.FrameSize)
	for {
		n, rerr := io.ReadFull(r, buf)
		if n > 0 {
			if err := st.Send(&dfsv1.UploadRequest{Msg: &dfsv1.UploadRequest_Data{Data: buf[:n]}}); err != nil {
				return nil, closeErr(st, err)
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return nil, rerr
		}
	}
	return st.CloseAndRecv()
}

func closeErr(st dfsv1.FileService_UploadClient, err error) error {
	if errors.Is(err, io.EOF) {
		if _, rerr := st.CloseAndRecv(); rerr != nil {
			return rerr
		}
	}
	return err
}

// Download writes the latest version of name to w, retrying elsewhere only if
// no bytes were written yet.
func (c *Client) Download(ctx context.Context, name string, w io.Writer) (*dfsv1.FileInfo, error) {
	var errs []error
	for _, addr := range c.order() {
		cw := &countingWriter{w: w}
		fi, err := c.DownloadFrom(ctx, addr, name, "", cw)
		if err == nil {
			return fi, nil
		}
		errs = append(errs, fmt.Errorf("%s: %w", addr, err))
		if cw.n > 0 || !rpcutil.IsUnavailable(err) || ctx.Err() != nil {
			break
		}
	}
	return nil, errors.Join(errs...)
}

func (c *Client) DownloadFrom(ctx context.Context, addr, name, version string, w io.Writer) (*dfsv1.FileInfo, error) {
	s, err := c.stub(addr)
	if err != nil {
		return nil, err
	}
	st, err := s.Download(ctx, &dfsv1.DownloadRequest{Name: name, Version: version})
	if err != nil {
		return nil, err
	}
	var info *dfsv1.FileInfo
	var got int64
	for {
		m, err := st.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if i := m.GetInfo(); i != nil {
			info = i
			continue
		}
		k, err := w.Write(m.GetData())
		got += int64(k)
		if err != nil {
			return nil, err
		}
	}
	if info == nil {
		return nil, errors.New("client: download stream had no file info")
	}
	if got != info.Size {
		return nil, fmt.Errorf("client: short download: %d of %d bytes", got, info.Size)
	}
	return info, nil
}

func (c *Client) Stat(ctx context.Context, name string) (*dfsv1.FileInfo, error) {
	var lastErr error
	for _, addr := range c.order() {
		s, err := c.stub(addr)
		if err != nil {
			return nil, err
		}
		fi, err := s.Stat(ctx, &dfsv1.StatRequest{Name: name})
		if err == nil || !rpcutil.IsUnavailable(err) {
			return fi, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *Client) List(ctx context.Context, prefix string, limit int) ([]*dfsv1.FileInfo, error) {
	var lastErr error
	for _, addr := range c.order() {
		s, err := c.stub(addr)
		if err != nil {
			return nil, err
		}
		resp, err := s.List(ctx, &dfsv1.ListRequest{Prefix: prefix, Limit: int32(limit)})
		if err == nil {
			return resp.Files, nil
		}
		if !rpcutil.IsUnavailable(err) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

func (c *Client) Delete(ctx context.Context, name string) error {
	var lastErr error
	for _, addr := range c.order() {
		s, err := c.stub(addr)
		if err != nil {
			return err
		}
		_, err = s.Delete(ctx, &dfsv1.DeleteRequest{Name: name})
		if err == nil || !rpcutil.IsUnavailable(err) {
			return err
		}
		lastErr = err
	}
	return lastErr
}

type sliceReader struct {
	b []byte
	i int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.i >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.i:])
	r.i += n
	return n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
