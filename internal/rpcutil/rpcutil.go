// Package rpcutil holds shared gRPC plumbing: tuned dial/server options, a
// connection pool keyed by address, and the leader-redirect convention used
// by the control plane.
package rpcutil

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// FrameSize is the data payload per streamed message.
const FrameSize = 1 << 20

const (
	maxMsg         = 8 << 20
	streamWindow   = 8 << 20
	connWindow     = 32 << 20
	socketBufBytes = 1 << 20
)

// ServerOptions are the options every gRPC server in the system uses. Large
// flow-control windows matter: with the 64 KiB default a single chunk stream
// stalls on window updates long before loopback or LAN bandwidth runs out.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMsg),
		grpc.MaxSendMsgSize(maxMsg),
		grpc.InitialWindowSize(streamWindow),
		grpc.InitialConnWindowSize(connWindow),
		grpc.ReadBufferSize(socketBufBytes),
		grpc.WriteBufferSize(socketBufBytes),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
	}
}

// DialOptions are the client counterpart of ServerOptions. Reconnect backoff
// is capped at 1s so a restarted node is usable again quickly.
func DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMsg), grpc.MaxCallSendMsgSize(maxMsg)),
		grpc.WithInitialWindowSize(streamWindow),
		grpc.WithInitialConnWindowSize(connWindow),
		grpc.WithReadBufferSize(socketBufBytes),
		grpc.WithWriteBufferSize(socketBufBytes),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: 20 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
			MinConnectTimeout: 2 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 3 * time.Second, PermitWithoutStream: true}),
	}
}

// Pool caches one ClientConn per address. gRPC multiplexes streams over it.
type Pool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

func NewPool(extra ...grpc.DialOption) *Pool {
	return &Pool{conns: map[string]*grpc.ClientConn{}, opts: append(DialOptions(), extra...)}
}

func (p *Pool) Get(addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.conns[addr]; ok {
		return c, nil
	}
	c, err := grpc.NewClient(addr, p.opts...)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = c
	return c, nil
}

// Forget closes and drops the connection to addr.
func (p *Pool) Forget(addr string) {
	p.mu.Lock()
	c, ok := p.conns[addr]
	delete(p.conns, addr)
	p.mu.Unlock()
	if ok {
		c.Close()
	}
}

func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for a, c := range p.conns {
		c.Close()
		delete(p.conns, a)
	}
}

// LeaderTrailer is the trailer key a follower sets to point at the leader.
const LeaderTrailer = "dfs-leader"

// NotLeader builds the error a follower returns for a leader-only RPC, and
// attaches the leader hint as a trailer.
func NotLeader(ctx context.Context, leaderAddr string) error {
	if leaderAddr != "" {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(LeaderTrailer, leaderAddr))
	}
	return status.Errorf(codes.FailedPrecondition, "not leader (leader=%q)", leaderAddr)
}

// IsNotLeader reports whether err came from NotLeader.
func IsNotLeader(err error) bool {
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.FailedPrecondition && strings.HasPrefix(s.Message(), "not leader")
}

// LeaderHint extracts the leader address from trailers collected with
// grpc.Trailer, or from the error message as a fallback.
func LeaderHint(md metadata.MD, err error) string {
	if v := md.Get(LeaderTrailer); len(v) > 0 && v[0] != "" {
		return v[0]
	}
	if s, ok := status.FromError(err); ok {
		msg := s.Message()
		if i := strings.Index(msg, `leader="`); i >= 0 {
			rest := msg[i+len(`leader="`):]
			if j := strings.IndexByte(rest, '"'); j > 0 {
				return rest[:j]
			}
		}
	}
	return ""
}

// IsUnavailable reports transport-level failures worth retrying elsewhere.
func IsUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted:
		return true
	}
	return false
}

// Listen opens a TCP listener; "127.0.0.1:0" picks a free port.
func Listen(addr string) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	return l, nil
}
