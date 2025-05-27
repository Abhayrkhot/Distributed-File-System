package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	wal "github.com/hashicorp/raft-wal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/repair"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

// Timing is the Raft failure-detection and election tuning.
type Timing struct {
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
}

// DefaultTiming is hashicorp/raft's stock configuration (WAN-safe; a crashed
// leader is replaced after roughly 1-3 s).
var DefaultTiming = Timing{
	HeartbeatTimeout:   1000 * time.Millisecond,
	ElectionTimeout:    1000 * time.Millisecond,
	LeaderLeaseTimeout: 500 * time.Millisecond,
	CommitTimeout:      50 * time.Millisecond,
}

// FastTiming is tuned for a single datacenter / LAN. A follower notices a
// silent leader after 200-400 ms (raft randomises the heartbeat timeout
// between 1x and 2x), or 100-200 ms after the leader's connection drops
// when FastCrashDetect is on.
//
// The election timeout is long on purpose. A candidate becomes leader the
// moment it has its votes; the timeout only decides when a *failed* election
// is retried. Each vote is fsynced, and under heavy disk load that can take
// longer than 100 ms, so a short election timeout makes candidates abandon
// elections that would have succeeded, and the cluster storms.
//
// 200 ms heartbeat and 100 ms lease were chosen from measurements: on the
// benchmark host, OS scheduling stalls reached ~250 ms under load, and the
// aggressive profile (100 ms / 50 ms) saw spurious elections there. See
// docs/results.md.
var FastTiming = Timing{
	HeartbeatTimeout:   200 * time.Millisecond,
	ElectionTimeout:    1000 * time.Millisecond,
	LeaderLeaseTimeout: 100 * time.Millisecond,
	CommitTimeout:      5 * time.Millisecond,
}

// AggressiveTiming halves the heartbeat timeout and lease. It recovers
// faster on an idle cluster but has spurious elections when the hosts stall
// for ~100 ms (seen under the 50-node load on one machine).
var AggressiveTiming = Timing{
	HeartbeatTimeout:   100 * time.Millisecond,
	ElectionTimeout:    1000 * time.Millisecond,
	LeaderLeaseTimeout: 50 * time.Millisecond,
	CommitTimeout:      5 * time.Millisecond,
}

// TimingProfile resolves a named profile.
func TimingProfile(name string) (Timing, error) {
	switch name {
	case "", "fast":
		return FastTiming, nil
	case "aggressive":
		return AggressiveTiming, nil
	case "default":
		return DefaultTiming, nil
	}
	return Timing{}, fmt.Errorf("unknown raft timing profile %q (want fast, aggressive or default)", name)
}

// Peer is one member of the Raft group.
type Peer struct {
	ID       string
	RaftAddr string
	GRPCAddr string
}

type Config struct {
	ID       string
	RaftBind string // e.g. 127.0.0.1:0
	// RaftAdvertise overrides the address peers dial (needed when binding to
	// 0.0.0.0, e.g. in Kubernetes). Defaults to the bound address.
	RaftAdvertise string
	GRPCBind      string
	GRPCAdvertise string
	// DataDir holds the BoltDB log/stable store and snapshots. Empty means
	// in-memory Raft storage (tests only).
	DataDir string
	// NoSync turns off fsync of the Raft log (benchmark comparison only).
	NoSync bool
	// LogStore selects the durable Raft storage: "wal" (default: raft-wal
	// segment log + single-fsync stable store) or "bolt" (raft-boltdb for
	// both, two fsyncs per write).
	LogStore string
	Timing   Timing
	// FastCrashDetect shortens the follower heartbeat timeout for a moment
	// when the leader's Raft connection is closed (see streamLayer).
	FastCrashDetect bool

	// DeadAfter is how long the leader waits without a heartbeat before
	// marking a storage node DOWN.
	DeadAfter time.Duration

	Replicas       int
	Metastore      metastore.Store // nil disables repair
	Blob           blob.Store
	RepairInterval time.Duration
	RepairDebounce time.Duration
	// ManualRepair: run repair only on TriggerRepair (tests).
	ManualRepair bool

	Logger    *slog.Logger
	RaftLevel string // hclog level for raft internals, default "warn"
}

type Server struct {
	cfg     Config
	log     *slog.Logger
	fsm     *FSM
	raft    *raft.Raft
	trans   *raft.NetworkTransport
	grpcLis net.Listener
	grpcSrv *grpc.Server
	stores  []io.Closer

	peersMu sync.RWMutex
	peers   map[string]Peer // by ID

	pool   *rpcutil.Pool
	repair *repair.Coordinator

	notifyCh chan bool
	leaderMu sync.Mutex
	leaderCx context.CancelFunc
	live     *liveness
	repairQ  chan chan *dfsv1.RepairStatus

	stopOnce sync.Once
	done     chan struct{}

	suspect     atomic.Bool
	FastDetects atomic.Int64 // times a closed connection shortened the timeout

	dfsv1.UnimplementedMetaServiceServer
}

// Open binds the Raft and gRPC listeners. Call Start with the full peer list
// once every member's addresses are known.
func Open(cfg Config) (*Server, error) {
	if cfg.ID == "" {
		return nil, errors.New("meta: empty node id")
	}
	if cfg.Timing == (Timing{}) {
		cfg.Timing = FastTiming
	}
	if cfg.DeadAfter == 0 {
		cfg.DeadAfter = 3 * time.Second
	}
	if cfg.Replicas == 0 {
		cfg.Replicas = 3
	}
	if cfg.RepairInterval == 0 {
		cfg.RepairInterval = 30 * time.Second
	}
	if cfg.RepairDebounce == 0 {
		cfg.RepairDebounce = 500 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RaftLevel == "" {
		cfg.RaftLevel = "warn"
	}
	s := &Server{
		cfg:      cfg,
		log:      cfg.Logger.With("meta", cfg.ID),
		fsm:      NewFSM(),
		peers:    map[string]Peer{},
		pool:     rpcutil.NewPool(),
		notifyCh: make(chan bool, 16),
		repairQ:  make(chan chan *dfsv1.RepairStatus),
		done:     make(chan struct{}),
	}

	var advertise net.Addr
	if cfg.RaftAdvertise != "" {
		// Kept as given (not resolved) so a stable DNS name, e.g. a
		// StatefulSet pod's, matches the address in the Raft configuration.
		if _, _, err := net.SplitHostPort(cfg.RaftAdvertise); err != nil {
			return nil, fmt.Errorf("meta: raft advertise: %w", err)
		}
		advertise = hostPortAddr(cfg.RaftAdvertise)
	}
	var onLost func()
	if cfg.FastCrashDetect {
		onLost = s.onPeerConnLost
	}
	sl, err := newStreamLayer(cfg.RaftBind, advertise, onLost)
	if err != nil {
		return nil, fmt.Errorf("meta: raft transport: %w", err)
	}
	trans := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream: sl, MaxPool: 3, Timeout: 2 * time.Second, Logger: s.hclog("raft-net"),
	})
	s.trans = trans

	lis, err := rpcutil.Listen(cfg.GRPCBind)
	if err != nil {
		trans.Close()
		return nil, err
	}
	s.grpcLis = lis
	return s, nil
}

func (s *Server) hclog(name string) hclog.Logger {
	return hclog.New(&hclog.LoggerOptions{Name: s.cfg.ID + "." + name, Level: hclog.LevelFromString(s.cfg.RaftLevel), Output: os.Stderr})
}

// RaftAddr is the address peers use for Raft traffic.
func (s *Server) RaftAddr() string { return string(s.trans.LocalAddr()) }

// GRPCAddr is the advertised gRPC address.
func (s *Server) GRPCAddr() string {
	if s.cfg.GRPCAdvertise != "" {
		return s.cfg.GRPCAdvertise
	}
	return s.grpcLis.Addr().String()
}

func (s *Server) ID() string { return s.cfg.ID }

// Start creates the Raft node, bootstraps the cluster if bootstrap is set and
// there is no existing state, and starts serving gRPC.
func (s *Server) Start(peers []Peer, bootstrap bool) error {
	s.peersMu.Lock()
	for _, p := range peers {
		s.peers[p.ID] = p
	}
	s.peersMu.Unlock()

	rc := raft.DefaultConfig()
	rc.LocalID = raft.ServerID(s.cfg.ID)
	rc.HeartbeatTimeout = s.cfg.Timing.HeartbeatTimeout
	rc.ElectionTimeout = s.cfg.Timing.ElectionTimeout
	rc.LeaderLeaseTimeout = s.cfg.Timing.LeaderLeaseTimeout
	rc.CommitTimeout = s.cfg.Timing.CommitTimeout
	rc.NotifyCh = s.notifyCh
	rc.Logger = s.hclog("raft")
	rc.SnapshotThreshold = 4096
	rc.TrailingLogs = 2048

	var (
		logs   raft.LogStore
		stable raft.StableStore
		snaps  raft.SnapshotStore
	)
	if s.cfg.DataDir == "" {
		mem := raft.NewInmemStore()
		logs, stable, snaps = mem, mem, raft.NewInmemSnapshotStore()
	} else {
		if err := os.MkdirAll(s.cfg.DataDir, 0o755); err != nil {
			return err
		}
		fss, err := raft.NewFileSnapshotStoreWithLogger(s.cfg.DataDir, 2, s.hclog("snap"))
		if err != nil {
			return err
		}
		snaps = fss
		switch s.cfg.LogStore {
		case "bolt":
			bolt, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(s.cfg.DataDir, "raft.db"), NoSync: s.cfg.NoSync})
			if err != nil {
				return fmt.Errorf("meta: bolt: %w", err)
			}
			s.stores = append(s.stores, bolt)
			logs, stable = bolt, bolt
		case "", "wal":
			if s.cfg.NoSync {
				return errors.New("meta: --raft-no-sync is only supported with the bolt log store")
			}
			walDir := filepath.Join(s.cfg.DataDir, "wal")
			if err := os.MkdirAll(walDir, 0o755); err != nil {
				return err
			}
			w, err := wal.Open(walDir, wal.WithSegmentSize(16<<20), wal.WithLogger(s.hclog("wal")))
			if err != nil {
				return fmt.Errorf("meta: wal: %w", err)
			}
			st, err := OpenFileStableStore(filepath.Join(s.cfg.DataDir, "stable"))
			if err != nil {
				w.Close()
				return err
			}
			s.stores = append(s.stores, w, st)
			logs, stable = w, st
		default:
			return fmt.Errorf("meta: unknown log store %q", s.cfg.LogStore)
		}
	}

	if bootstrap {
		has, err := raft.HasExistingState(logs, stable, snaps)
		if err != nil {
			return err
		}
		if !has {
			var servers []raft.Server
			for _, p := range peers {
				servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.RaftAddr), Suffrage: raft.Voter})
			}
			if err := raft.BootstrapCluster(rc, logs, stable, snaps, s.trans, raft.Configuration{Servers: servers}); err != nil {
				return fmt.Errorf("meta: bootstrap: %w", err)
			}
		}
	}
	r, err := raft.NewRaft(rc, s.fsm, logs, stable, snaps, s.trans)
	if err != nil {
		return fmt.Errorf("meta: raft: %w", err)
	}
	s.raft = r
	s.repair = repair.New(repair.Config{
		Replicas: s.cfg.Replicas, Meta: s.cfg.Metastore, Blob: s.cfg.Blob, Pool: s.pool,
		Logger: s.log.With("component", "repair"),
	})

	s.grpcSrv = grpc.NewServer(rpcutil.ServerOptions()...)
	dfsv1.RegisterMetaServiceServer(s.grpcSrv, s)
	go s.grpcSrv.Serve(s.grpcLis)
	go s.watchLeadership()
	return nil
}

// Raft exposes the underlying node (tests and benchmarks).
func (s *Server) Raft() *raft.Raft { return s.raft }

// FSM exposes the replicated state.
func (s *Server) FSM() *FSM { return s.fsm }

func (s *Server) IsLeader() bool { return s.raft != nil && s.raft.State() == raft.Leader }

// leaderGRPC returns the leader's gRPC address, if known.
func (s *Server) leaderGRPC() (string, string) {
	_, id := s.raft.LeaderWithID()
	if id == "" {
		return "", ""
	}
	s.peersMu.RLock()
	defer s.peersMu.RUnlock()
	return s.peers[string(id)].GRPCAddr, string(id)
}

// Shutdown stops the node. With graceful=true a leader first hands over
// leadership, which avoids an election timeout for planned restarts.
func (s *Server) Shutdown(graceful bool) {
	s.stopOnce.Do(func() {
		close(s.done)
		if s.raft != nil && graceful && s.IsLeader() {
			if err := s.raft.LeadershipTransfer().Error(); err != nil {
				s.log.Warn("leadership transfer failed", "err", err)
			}
		}
		s.stepDown()
		if s.grpcSrv != nil {
			if graceful {
				stopped := make(chan struct{})
				go func() { s.grpcSrv.GracefulStop(); close(stopped) }()
				select {
				case <-stopped:
				case <-time.After(2 * time.Second):
					s.grpcSrv.Stop()
				}
			} else {
				s.grpcSrv.Stop()
			}
		} else {
			s.grpcLis.Close()
		}
		if s.raft != nil {
			s.raft.Shutdown().Error()
		}
		s.trans.Close()
		for _, c := range s.stores {
			c.Close()
		}
		s.pool.Close()
	})
}

// onPeerConnLost runs when an inbound Raft connection is closed by its peer.
// On a follower, that is usually the leader's replication stream, and a
// closed one usually means the leader process died. For a short window the
// follower checks for leader contact with a shorter timeout, so it starts an
// election after ~LeaderLeaseTimeout of silence instead of 1-2x the full
// HeartbeatTimeout. If the leader is in fact alive it keeps heartbeating
// (every HeartbeatTimeout/10), so nothing happens; and if one follower does
// start an election wrongly, pre-vote lets the others, which still hear the
// leader, refuse it.
func (s *Server) onPeerConnLost() {
	r := s.raft
	if r == nil || r.State() != raft.Follower {
		return
	}
	if !s.suspect.CompareAndSwap(false, true) {
		return
	}
	s.FastDetects.Add(1)
	cur := r.ReloadableConfig()
	fast := cur
	fast.HeartbeatTimeout = max(s.cfg.Timing.LeaderLeaseTimeout, s.cfg.Timing.HeartbeatTimeout/2)
	if err := r.ReloadConfig(fast); err != nil {
		s.log.Debug("fast crash detect: reload failed", "err", err)
		s.suspect.Store(false)
		return
	}
	time.AfterFunc(10*s.cfg.Timing.HeartbeatTimeout, func() {
		select {
		case <-s.done:
			return
		default:
		}
		restore := r.ReloadableConfig()
		restore.HeartbeatTimeout = s.cfg.Timing.HeartbeatTimeout
		if err := r.ReloadConfig(restore); err != nil {
			s.log.Debug("fast crash detect: restore failed", "err", err)
		}
		s.suspect.Store(false)
	})
}

// ----------------------------------------------------------------------
// Leader duties
// ----------------------------------------------------------------------

func (s *Server) watchLeadership() {
	for {
		select {
		case <-s.done:
			return
		case isLeader := <-s.notifyCh:
			if isLeader {
				s.stepUp()
			} else {
				s.stepDown()
			}
		}
	}
}

func (s *Server) stepUp() {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	if s.leaderCx != nil {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.leaderCx = cancel
	s.live = newLiveness(s.fsm.Nodes(), time.Now())
	s.log.Info("became leader", "term", s.raft.Stats()["term"])
	go s.livenessLoop(ctx, s.live)
	if s.cfg.Metastore != nil {
		go s.repairLoop(ctx)
	}
}

func (s *Server) stepDown() {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	if s.leaderCx != nil {
		s.leaderCx()
		s.leaderCx = nil
		s.live = nil
		s.log.Info("lost leadership")
	}
}

func (s *Server) liveTracker() *liveness {
	s.leaderMu.Lock()
	defer s.leaderMu.Unlock()
	return s.live
}

func (s *Server) apply(ctx context.Context, c command) (applyResult, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return applyResult{}, err
	}
	timeout := 2 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}
	f := s.raft.Apply(b, timeout)
	if err := f.Error(); err != nil {
		return applyResult{}, err
	}
	switch v := f.Response().(type) {
	case applyResult:
		return v, nil
	case error:
		return applyResult{}, v
	}
	return applyResult{}, nil
}

// livenessLoop marks storage nodes DOWN when their heartbeats stop.
func (s *Server) livenessLoop(ctx context.Context, lv *liveness) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, n := range s.fsm.Nodes() {
				if n.State != dfsv1.NodeState_NODE_STATE_UP {
					continue
				}
				if now.Sub(lv.lastSeen(n.ID, now)) > s.cfg.DeadAfter {
					s.log.Warn("storage node missed heartbeats; marking DOWN", "node", n.ID, "addr", n.Addr)
					if _, err := s.apply(ctx, command{Op: opState, ID: n.ID, State: dfsv1.NodeState_NODE_STATE_DOWN, TS: now.UnixMilli()}); err != nil {
						s.log.Warn("mark down failed", "node", n.ID, "err", err)
					}
				}
			}
		}
	}
}

func (s *Server) repairLoop(ctx context.Context) {
	tick := time.NewTicker(s.cfg.RepairInterval)
	defer tick.Stop()
	var debounce <-chan time.Time
	changed := s.fsm.Changed()
	run := func() *dfsv1.RepairStatus {
		r := s.fsm.Ring()
		if _, err := s.repair.Run(ctx, r); err != nil && ctx.Err() == nil {
			s.log.Warn("repair pass failed", "err", err)
		}
		return s.repair.Status()
	}
	// A new leader does not know what happened before it was elected.
	debounce = time.After(s.cfg.RepairDebounce)
	if s.cfg.ManualRepair {
		debounce = nil
		tick.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-changed:
			changed = s.fsm.Changed()
			if !s.cfg.ManualRepair {
				debounce = time.After(s.cfg.RepairDebounce)
			}
		case <-debounce:
			debounce = nil
			run()
		case <-tick.C:
			run()
		case reply := <-s.repairQ:
			reply <- run()
		}
	}
}

// ----------------------------------------------------------------------
// gRPC
// ----------------------------------------------------------------------

func (s *Server) notLeader(ctx context.Context) error {
	addr, _ := s.leaderGRPC()
	return rpcutil.NotLeader(ctx, addr)
}

func (s *Server) Heartbeat(ctx context.Context, req *dfsv1.HeartbeatRequest) (*dfsv1.HeartbeatResponse, error) {
	lv := s.liveTracker()
	if !s.IsLeader() || lv == nil {
		return nil, s.notLeader(ctx)
	}
	if req.NodeId == "" || req.Addr == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and addr are required")
	}
	now := time.Now()
	lv.seen(req.NodeId, now)
	n, ok := s.fsm.Node(req.NodeId)
	if !ok || n.Addr != req.Addr || n.State != dfsv1.NodeState_NODE_STATE_UP {
		if ok && n.State == dfsv1.NodeState_NODE_STATE_DOWN {
			s.log.Info("storage node is back; marking UP", "node", req.NodeId)
		} else if !ok {
			s.log.Info("storage node joined", "node", req.NodeId, "addr", req.Addr)
		}
		if _, err := s.apply(ctx, command{Op: opUpsert, ID: req.NodeId, Addr: req.Addr, TS: now.UnixMilli()}); err != nil {
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return nil, s.notLeader(ctx)
			}
			return nil, status.Errorf(codes.Unavailable, "apply: %v", err)
		}
	}
	return &dfsv1.HeartbeatResponse{RingEpoch: s.fsm.Epoch()}, nil
}

func (s *Server) GetRing(ctx context.Context, _ *dfsv1.GetRingRequest) (*dfsv1.Ring, error) {
	return s.fsm.RingProto(), nil
}

func (s *Server) WatchRing(req *dfsv1.WatchRingRequest, stream dfsv1.MetaService_WatchRingServer) error {
	after := req.AfterEpoch
	sent := false
	for {
		ch := s.fsm.Changed()
		rp := s.fsm.RingProto()
		if rp.Epoch > after || (!sent && after == 0) {
			if err := stream.Send(rp); err != nil {
				return err
			}
			after, sent = rp.Epoch, true
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-s.done:
			return status.Error(codes.Unavailable, "shutting down")
		case <-ch:
		}
	}
}

func (s *Server) Status(ctx context.Context, _ *dfsv1.StatusRequest) (*dfsv1.ClusterStatus, error) {
	stats := s.raft.Stats()
	leaderAddr, leaderID := s.leaderGRPC()
	cs := &dfsv1.ClusterStatus{
		SelfId:         s.cfg.ID,
		LeaderId:       leaderID,
		LeaderGrpcAddr: leaderAddr,
		Ring:           s.fsm.RingProto(),
		RaftState:      s.raft.State().String(),
		Term:           parseU64(stats["term"]),
		CommitIndex:    s.raft.CommitIndex(),
		AppliedIndex:   s.raft.AppliedIndex(),
		Repair:         s.repair.Status(),
	}
	if f := s.raft.GetConfiguration(); f.Error() == nil {
		s.peersMu.RLock()
		for _, srv := range f.Configuration().Servers {
			p := s.peers[string(srv.ID)]
			cs.Peers = append(cs.Peers, &dfsv1.MetaPeer{Id: string(srv.ID), RaftAddr: string(srv.Address), GrpcAddr: p.GRPCAddr, Leader: string(srv.ID) == leaderID})
		}
		s.peersMu.RUnlock()
	}
	return cs, nil
}

func (s *Server) Propose(ctx context.Context, _ *dfsv1.ProposeRequest) (*dfsv1.ProposeResponse, error) {
	if !s.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	if _, err := s.apply(ctx, command{Op: opNoop, TS: time.Now().UnixMilli()}); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, s.notLeader(ctx)
		}
		return nil, status.Errorf(codes.Unavailable, "apply: %v", err)
	}
	return &dfsv1.ProposeResponse{Index: s.raft.AppliedIndex(), LeaderId: s.cfg.ID}, nil
}

func (s *Server) TriggerRepair(ctx context.Context, req *dfsv1.TriggerRepairRequest) (*dfsv1.RepairStatus, error) {
	if !s.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	if s.cfg.Metastore == nil {
		return nil, status.Error(codes.FailedPrecondition, "repair disabled: no metastore configured")
	}
	reply := make(chan *dfsv1.RepairStatus, 1)
	select {
	case s.repairQ <- reply:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if !req.Wait {
		return s.repair.Status(), nil
	}
	select {
	case st := <-reply:
		return st, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Server) TransferLeadership(ctx context.Context, _ *dfsv1.TransferLeadershipRequest) (*dfsv1.TransferLeadershipResponse, error) {
	if !s.IsLeader() {
		return nil, s.notLeader(ctx)
	}
	if err := s.raft.LeadershipTransfer().Error(); err != nil {
		return nil, status.Errorf(codes.Unavailable, "transfer: %v", err)
	}
	_, id := s.raft.LeaderWithID()
	return &dfsv1.TransferLeadershipResponse{NewLeaderId: string(id)}, nil
}

func parseU64(s string) uint64 {
	var v uint64
	fmt.Sscan(s, &v)
	return v
}

// ----------------------------------------------------------------------

// liveness is leader-local soft state: when each storage node last
// heartbeated. A new leader starts every known node at "now", giving nodes a
// full DeadAfter window to find it before anything is marked DOWN.
type liveness struct {
	mu   sync.Mutex
	last map[string]time.Time
	born time.Time
}

func newLiveness(nodes []nodeRec, now time.Time) *liveness {
	l := &liveness{last: map[string]time.Time{}, born: now}
	for _, n := range nodes {
		l.last[n.ID] = now
	}
	return l
}

func (l *liveness) seen(id string, t time.Time) {
	l.mu.Lock()
	l.last[id] = t
	l.mu.Unlock()
}

func (l *liveness) lastSeen(id string, now time.Time) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[id]; ok {
		return t
	}
	return l.born
}
