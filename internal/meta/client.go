package meta

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/ring"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

// Client talks to the control plane. Leader-only calls follow the leader
// hint that followers return, and rotate through the peers when the hinted
// node is unreachable, so callers see one call that succeeds as soon as any
// leader is reachable.
type Client struct {
	addrs []string
	pool  *rpcutil.Pool
	own   bool

	// RetryInterval is the pause between attempts while no leader is known.
	RetryInterval time.Duration
	// AttemptTimeout bounds each individual RPC attempt.
	AttemptTimeout time.Duration

	mu     sync.Mutex
	leader string
	next   int
}

// NewClient creates a client for the given meta gRPC addresses. pool may be
// nil.
func NewClient(addrs []string, pool *rpcutil.Pool) *Client {
	c := &Client{addrs: append([]string(nil), addrs...), pool: pool, RetryInterval: 10 * time.Millisecond, AttemptTimeout: time.Second}
	if c.pool == nil {
		c.pool = rpcutil.NewPool()
		c.own = true
	}
	return c
}

func (c *Client) Close() {
	if c.own {
		c.pool.Close()
	}
}

// Addrs returns the configured meta addresses.
func (c *Client) Addrs() []string { return append([]string(nil), c.addrs...) }

func (c *Client) stub(addr string) (dfsv1.MetaServiceClient, error) {
	conn, err := c.pool.Get(addr)
	if err != nil {
		return nil, err
	}
	return dfsv1.NewMetaServiceClient(conn), nil
}

func (c *Client) pick() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.leader != "" {
		return c.leader
	}
	a := c.addrs[c.next%len(c.addrs)]
	c.next++
	return a
}

func (c *Client) setLeader(addr string) {
	c.mu.Lock()
	c.leader = addr
	c.mu.Unlock()
}

// KnownLeader returns the cached leader address, if any.
func (c *Client) KnownLeader() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.leader
}

// onLeader runs fn against the leader, retrying until ctx expires.
func (c *Client) onLeader(ctx context.Context, fn func(context.Context, dfsv1.MetaServiceClient, ...grpc.CallOption) error) error {
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return errors.Join(err, lastErr)
			}
			return err
		}
		addr := c.pick()
		stub, err := c.stub(addr)
		if err != nil {
			return err
		}
		var md metadata.MD
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		err = fn(actx, stub, grpc.Trailer(&md))
		cancel()
		if err == nil {
			c.setLeader(addr)
			return nil
		}
		lastErr = err
		switch {
		case rpcutil.IsNotLeader(err):
			hint := rpcutil.LeaderHint(md, err)
			if hint != "" && hint != addr {
				c.setLeader(hint)
				continue // follow the redirect immediately
			}
			c.setLeader("")
		case rpcutil.IsUnavailable(err):
			c.setLeader("")
		default:
			return err
		}
		select {
		case <-ctx.Done():
		case <-time.After(c.RetryInterval):
		}
	}
}

func (c *Client) Heartbeat(ctx context.Context, req *dfsv1.HeartbeatRequest) (*dfsv1.HeartbeatResponse, error) {
	var resp *dfsv1.HeartbeatResponse
	err := c.onLeader(ctx, func(ctx context.Context, s dfsv1.MetaServiceClient, o ...grpc.CallOption) error {
		var err error
		resp, err = s.Heartbeat(ctx, req, o...)
		return err
	})
	return resp, err
}

// Propose commits a no-op through Raft and returns the leader that did it.
func (c *Client) Propose(ctx context.Context) (*dfsv1.ProposeResponse, error) {
	var resp *dfsv1.ProposeResponse
	err := c.onLeader(ctx, func(ctx context.Context, s dfsv1.MetaServiceClient, o ...grpc.CallOption) error {
		var err error
		resp, err = s.Propose(ctx, &dfsv1.ProposeRequest{}, o...)
		return err
	})
	return resp, err
}

func (c *Client) TriggerRepair(ctx context.Context, wait bool) (*dfsv1.RepairStatus, error) {
	var resp *dfsv1.RepairStatus
	saved := c.AttemptTimeout
	err := c.onLeader(ctx, func(actx context.Context, s dfsv1.MetaServiceClient, o ...grpc.CallOption) error {
		var err error
		// A waited repair pass can outlast the per-attempt timeout.
		callCtx := actx
		if wait {
			callCtx = ctx
		}
		resp, err = s.TriggerRepair(callCtx, &dfsv1.TriggerRepairRequest{Wait: wait}, o...)
		return err
	})
	c.AttemptTimeout = saved
	return resp, err
}

func (c *Client) TransferLeadership(ctx context.Context) (*dfsv1.TransferLeadershipResponse, error) {
	var resp *dfsv1.TransferLeadershipResponse
	err := c.onLeader(ctx, func(ctx context.Context, s dfsv1.MetaServiceClient, o ...grpc.CallOption) error {
		var err error
		resp, err = s.TransferLeadership(ctx, &dfsv1.TransferLeadershipRequest{}, o...)
		return err
	})
	return resp, err
}

// any calls fn against each peer in turn until one answers.
func (c *Client) any(ctx context.Context, fn func(context.Context, dfsv1.MetaServiceClient) error) error {
	var lastErr error
	for i := 0; i < len(c.addrs); i++ {
		addr := c.pick()
		stub, err := c.stub(addr)
		if err != nil {
			return err
		}
		actx, cancel := context.WithTimeout(ctx, c.AttemptTimeout)
		err = fn(actx, stub)
		cancel()
		if err == nil {
			return nil
		}
		lastErr = err
		c.setLeader("")
	}
	return lastErr
}

// GetRing returns the routing ring from any reachable peer.
func (c *Client) GetRing(ctx context.Context) (*ring.Ring, *dfsv1.Ring, error) {
	var rp *dfsv1.Ring
	err := c.any(ctx, func(ctx context.Context, s dfsv1.MetaServiceClient) error {
		var err error
		rp, err = s.GetRing(ctx, &dfsv1.GetRingRequest{})
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return RingFromProto(rp), rp, nil
}

// Status asks one specific peer for its view.
func (c *Client) Status(ctx context.Context, addr string) (*dfsv1.ClusterStatus, error) {
	stub, err := c.stub(addr)
	if err != nil {
		return nil, err
	}
	return stub.Status(ctx, &dfsv1.StatusRequest{})
}

// LeaderStatus returns the leader's status.
func (c *Client) LeaderStatus(ctx context.Context) (*dfsv1.ClusterStatus, error) {
	var st *dfsv1.ClusterStatus
	err := c.onLeader(ctx, func(ctx context.Context, s dfsv1.MetaServiceClient, o ...grpc.CallOption) error {
		var err error
		st, err = s.Status(ctx, &dfsv1.StatusRequest{}, o...)
		if err == nil && st.RaftState != "Leader" {
			if st.LeaderGrpcAddr != "" {
				c.setLeader(st.LeaderGrpcAddr)
			}
			return rpcutil.NotLeader(ctx, st.LeaderGrpcAddr)
		}
		return err
	})
	return st, err
}

// WatchRing keeps a WatchRing stream open to some peer and calls fn with
// every new ring, reconnecting (to the next peer) when a stream breaks. It
// returns when ctx is cancelled.
func (c *Client) WatchRing(ctx context.Context, fn func(*ring.Ring, *dfsv1.Ring)) {
	var after uint64
	for ctx.Err() == nil {
		addr := c.pick()
		stub, err := c.stub(addr)
		if err == nil {
			var stream dfsv1.MetaService_WatchRingClient
			stream, err = stub.WatchRing(ctx, &dfsv1.WatchRingRequest{AfterEpoch: after})
			for err == nil {
				var rp *dfsv1.Ring
				rp, err = stream.Recv()
				if err == nil && rp.Epoch >= after {
					after = rp.Epoch
					fn(RingFromProto(rp), rp)
				}
			}
		}
		c.setLeader("")
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}
