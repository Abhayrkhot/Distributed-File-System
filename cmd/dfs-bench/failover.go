package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/localcluster"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
)

type FailoverResult struct {
	Env        Env       `json:"env"`
	Mode       string    `json:"mode"`
	Timing     string    `json:"timing"`
	RaftStore  string    `json:"raft_store"`
	FastDetect bool      `json:"fast_crash_detect"`
	RaftSync   bool      `json:"raft_fsync"`
	MetaNodes  int       `json:"meta_nodes"`
	Storage    int       `json:"storage_nodes"`
	Trials     int       `json:"trials"`
	Definition string    `json:"definition"`
	RecoveryMs Dist      `json:"recovery_ms"`
	SamplesMs  []float64 `json:"samples_ms"`
	Failed     int       `json:"failed_trials"`
}

// runFailover repeatedly removes the Raft leader and measures the time until
// the control plane commits a write again, as seen by a client:
//
//	t0 = just before SIGKILL (crash) / SIGTERM (graceful) / transfer RPC
//	t1 = first Propose (a no-op committed through Raft) that succeeds
//
// The client starts with the old leader cached, so the measurement includes
// discovering the new leader (via follower redirects).
func runFailover(args []string) error {
	fs := flag.NewFlagSet("failover", flag.ExitOnError)
	trials := fs.Int("trials", 30, "number of leader failures")
	timing := fs.String("timing", "fast", "raft timing profile passed to dfs-meta (fast|default)")
	mode := fs.String("mode", "crash", "crash (SIGKILL), freeze (SIGSTOP: sockets stay open), term (SIGTERM: graceful handover) or transfer (leadership-transfer RPC)")
	store := fs.String("raft-store", "wal", "Raft storage passed to dfs-meta: wal or bolt")
	fastDetect := fs.Bool("fast-crash-detect", true, "enable connection-loss crash detection in dfs-meta")
	metaN := fs.Int("meta", 3, "control-plane members")
	storageN := fs.Int("storage", 10, "storage nodes heartbeating to the leader during the run")
	noSync := fs.Bool("raft-no-sync", false, "disable Raft log fsync")
	settle := fs.Duration("settle", 1500*time.Millisecond, "pause after a member rejoins before the next trial")
	out := fs.String("out", "bench/results", "results directory")
	bin := fs.String("bin", repoBin(), "directory with dfs-meta and dfs-storage")
	fs.Parse(args)

	dir, err := os.MkdirTemp("", "dfs-failover-*.noindex")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	metaArgs := []string{"--timing", *timing, "--raft-log-level", "error", "--log-level", "warn",
		"--raft-store", *store, fmt.Sprintf("--fast-crash-detect=%v", *fastDetect)}
	if *noSync {
		metaArgs = append(metaArgs, "--raft-no-sync")
	}
	ctx := context.Background()
	fmt.Printf("starting %d meta + %d storage processes (timing=%s, store=%s, fast-detect=%v, mode=%s)\n", *metaN, *storageN, *timing, *store, *fastDetect, *mode)
	c, err := localcluster.Start(ctx, localcluster.Options{
		BinDir: *bin, Dir: dir, Meta: *metaN, Storage: *storageN,
		MetaArgs:    metaArgs,
		StorageArgs: []string{"--log-level", "error"},
	})
	if err != nil {
		return err
	}
	defer c.Stop()

	mc := meta.NewClient(c.MetaAddrs, nil)
	defer mc.Close()
	mc.RetryInterval = time.Millisecond
	mc.AttemptTimeout = 100 * time.Millisecond

	res := FailoverResult{Env: hostEnv(), Mode: *mode, Timing: *timing, RaftSync: !*noSync, RaftStore: *store, FastDetect: *fastDetect, MetaNodes: *metaN,
		Storage: *storageN, Trials: *trials,
		Definition: "time from removing the leader (signal or transfer RPC) until a client's first successful Raft-committed write, including new-leader discovery"}

	for i := 0; i < *trials; i++ {
		wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		st, err := mc.LeaderStatus(wctx)
		cancel()
		if err != nil {
			return fmt.Errorf("trial %d: no leader before trial: %w", i, err)
		}
		old := c.MetaByID(st.LeaderId)
		// Make sure the client has the old leader cached, as a real client would.
		if _, err := mc.Propose(ctx); err != nil {
			return err
		}

		t0 := time.Now()
		switch *mode {
		case "crash":
			err = old.Kill()
		case "freeze":
			err = old.Freeze()
		case "term":
			go old.Terminate(10 * time.Second)
		case "transfer":
			go mc.TransferLeadership(ctx)
		default:
			return fmt.Errorf("unknown mode %q", *mode)
		}
		if err != nil {
			return err
		}
		pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		var resp interface{ GetLeaderId() string }
		for {
			r, perr := mc.Propose(pctx)
			if perr != nil {
				err = perr
				break
			}
			if r.GetLeaderId() != st.LeaderId {
				resp = r
				break
			}
			// Still the old leader (transfer not done yet): keep probing.
		}
		took := time.Since(t0)
		cancel()
		if resp == nil {
			res.Failed++
			fmt.Printf("trial %2d: FAILED (%v)\n", i+1, err)
		} else {
			res.SamplesMs = append(res.SamplesMs, ms(took))
			fmt.Printf("trial %2d: %s -> %s in %7.1f ms\n", i+1, st.LeaderId, resp.GetLeaderId(), ms(took))
		}

		if *mode == "freeze" {
			old.Kill() // a stopped process still dies on SIGKILL
		}
		if *mode != "transfer" {
			for old.Running() {
				time.Sleep(10 * time.Millisecond)
			}
			if err := old.Start(); err != nil {
				return err
			}
		}
		// Wait for the member to rejoin and catch up before the next trial.
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			sctx, cancel := context.WithTimeout(ctx, time.Second)
			lst, err1 := mc.LeaderStatus(sctx)
			ost, err2 := mc.Status(sctx, old.Addr)
			cancel()
			if err1 == nil && err2 == nil && ost.AppliedIndex+5 >= lst.CommitIndex && ost.LeaderId != "" {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(*settle)
	}
	res.RecoveryMs = dist(res.SamplesMs)
	fmt.Printf("\nrecovery (ms): %s  failed=%d\n", res.RecoveryMs, res.Failed)
	name := fmt.Sprintf("failover-%s-%s-%s%s%s.json", *mode, *timing, *store,
		map[bool]string{true: "-nosync", false: ""}[*noSync], map[bool]string{true: "", false: "-nodetect"}[*fastDetect])
	p, err := writeJSON(*out, name, res)
	if err != nil {
		return err
	}
	fmt.Println("wrote", filepath.Clean(p))
	return nil
}
