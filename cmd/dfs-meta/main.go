// dfs-meta runs one member of the Raft control plane.
//
//	dfs-meta --id meta-0 --raft-bind 127.0.0.1:7100 --grpc-bind 127.0.0.1:8100 \
//	  --peers meta-0=127.0.0.1:7100=127.0.0.1:8100,meta-1=...,meta-2=... \
//	  --bootstrap --data-dir /var/lib/dfs/meta-0 --cassandra 127.0.0.1:9042
//
// SIGTERM/SIGINT hand leadership over before exiting; SIGKILL is a crash.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/backend"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

func main() {
	var (
		id            = flag.String("id", os.Getenv("DFS_ID"), "member id (env DFS_ID)")
		raftBind      = flag.String("raft-bind", "127.0.0.1:7100", "Raft listen address")
		raftAdvertise = flag.String("raft-advertise", "", "Raft address peers dial, if different from --raft-bind")
		grpcBind      = flag.String("grpc-bind", "127.0.0.1:8100", "gRPC listen address")
		grpcAdvertise = flag.String("grpc-advertise", "", "gRPC address clients dial, if different")
		peers         = flag.String("peers", os.Getenv("DFS_PEERS"), "all members: id=raftAddr=grpcAddr,... (env DFS_PEERS)")
		bootstrap     = flag.Bool("bootstrap", false, "bootstrap the group if this member has no Raft state")
		dataDir       = flag.String("data-dir", "", "directory for the Raft log and snapshots (empty: in-memory)")
		noSync        = flag.Bool("raft-no-sync", false, "skip fsync on the Raft log (unsafe; bolt store only; for comparison benchmarks)")
		logStore      = flag.String("raft-store", "wal", "Raft storage: wal (segment log + single-fsync stable store) or bolt")
		fastDetect    = flag.Bool("fast-crash-detect", true, "shorten the follower heartbeat timeout when the leader's connection closes")
		timing        = flag.String("timing", "fast", "Raft timing profile: fast (LAN-tuned) or default (hashicorp defaults)")
		hb            = flag.Duration("heartbeat-timeout", 0, "override Raft heartbeat timeout")
		el            = flag.Duration("election-timeout", 0, "override Raft election timeout")
		lease         = flag.Duration("leader-lease", 0, "override Raft leader lease timeout")
		deadAfter     = flag.Duration("dead-after", 3*time.Second, "mark a storage node DOWN after this long without a heartbeat")
		replicas      = flag.Int("replicas", 3, "replication factor the repair pass maintains")
		repairEvery   = flag.Duration("repair-interval", 30*time.Second, "periodic repair pass interval")
		logLevel      = flag.String("log-level", "info", "debug, info, warn, error")
		raftLevel     = flag.String("raft-log-level", "warn", "hashicorp/raft log level")
		be            backend.Flags
	)
	be.Register(flag.CommandLine)
	flag.Parse()
	log := backend.Logger(*logLevel)

	if *id == "" {
		fatal("--id is required")
	}
	peerList, err := parsePeers(*peers)
	if err != nil {
		fatal(err.Error())
	}
	t, err := meta.TimingProfile(*timing)
	if err != nil {
		fatal(err.Error())
	}
	if *hb > 0 {
		t.HeartbeatTimeout = *hb
	}
	if *el > 0 {
		t.ElectionTimeout = *el
	}
	if *lease > 0 {
		t.LeaderLeaseTimeout = *lease
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	ms, err := be.Metastore(ctx, log)
	if err != nil {
		fatal(fmt.Sprintf("metastore: %v", err))
	}
	bs, err := be.Blob(ctx, log)
	cancel()
	if err != nil {
		fatal(fmt.Sprintf("blob: %v", err))
	}
	if ms == nil {
		log.Warn("no metastore configured: repair is disabled on this member")
	}

	s, err := meta.Open(meta.Config{
		ID: *id, RaftBind: *raftBind, RaftAdvertise: *raftAdvertise, GRPCBind: *grpcBind, GRPCAdvertise: *grpcAdvertise,
		DataDir: *dataDir, NoSync: *noSync, LogStore: *logStore, FastCrashDetect: *fastDetect, Timing: t, DeadAfter: *deadAfter, Replicas: *replicas,
		Metastore: ms, Blob: bs, RepairInterval: *repairEvery, Logger: log, RaftLevel: *raftLevel,
	})
	if err != nil {
		fatal(err.Error())
	}
	if len(peerList) == 0 {
		peerList = []meta.Peer{{ID: *id, RaftAddr: s.RaftAddr(), GRPCAddr: s.GRPCAddr()}}
	}
	if err := s.Start(peerList, *bootstrap); err != nil {
		fatal(err.Error())
	}
	log.Info("dfs-meta started", "id", *id, "raft", s.RaftAddr(), "grpc", s.GRPCAddr(), "timing", *timing, "store", *logStore, "fast_crash_detect", *fastDetect,
		"heartbeat_timeout", t.HeartbeatTimeout, "election_timeout", t.ElectionTimeout)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Info("shutting down (graceful leadership transfer)")
	s.Shutdown(true)
	if ms != nil {
		ms.Close()
	}
}

func parsePeers(s string) ([]meta.Peer, error) {
	if s == "" {
		return nil, nil
	}
	var out []meta.Peer
	for _, p := range strings.Split(s, ",") {
		parts := strings.Split(strings.TrimSpace(p), "=")
		if len(parts) != 3 {
			return nil, fmt.Errorf("bad peer %q: want id=raftAddr=grpcAddr", p)
		}
		out = append(out, meta.Peer{ID: parts[0], RaftAddr: parts[1], GRPCAddr: parts[2]})
	}
	return out, nil
}

func fatal(msg string) {
	fmt.Fprintln(os.Stderr, "dfs-meta:", msg)
	os.Exit(1)
}
