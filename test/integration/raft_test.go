package integration

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
)

// probeRecovery kills/transfers and measures how long until a write commits
// through Raft again.
func timeToCommit(ctx context.Context, c *cluster.Cluster) (time.Duration, error) {
	start := time.Now()
	c.MetaClient.RetryInterval = 2 * time.Millisecond
	c.MetaClient.AttemptTimeout = 200 * time.Millisecond
	_, err := c.MetaClient.Propose(ctx)
	return time.Since(start), err
}

// Crashing the Raft leader: a new leader is elected and commits writes within
// a few hundred ms with the fast timing profile, and the data plane serves
// uploads and downloads throughout without a single error.
func TestLeaderCrashFailover(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk})
	ctx := ctxT(t, 2*time.Minute)
	if _, err := c.MetaClient.Propose(ctx); err != nil {
		t.Fatal(err)
	}

	// Background data-plane traffic.
	var stop atomic.Bool
	var ops, failures atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; !stop.Load(); i++ {
				data := randomBytes(2*testChunk + i)
				name := fmt.Sprintf("bg-%d-%d", w, i)
				if _, err := c.Client.UploadBytes(ctx, name, data); err != nil {
					failures.Add(1)
					t.Errorf("upload during failover: %v", err)
					continue
				}
				var buf bytes.Buffer
				if _, err := c.Client.Download(ctx, name, &buf); err != nil || !bytes.Equal(buf.Bytes(), data) {
					failures.Add(1)
					t.Errorf("download during failover: %v", err)
				}
				ops.Add(1)
			}
		}(w)
	}

	old := c.Leader()
	time.Sleep(200 * time.Millisecond)
	c.KillMeta(old)
	took, err := timeToCommit(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	newLeader := c.Leader()
	if newLeader < 0 || newLeader == old {
		t.Fatalf("no new leader (old=%d new=%d)", old, newLeader)
	}
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	t.Logf("leader meta-%d killed; meta-%d committing after %v; %d data-plane ops during failover, %d failures",
		old, newLeader, took.Round(time.Millisecond), ops.Load(), failures.Load())
	if took > 2*time.Second {
		t.Fatalf("failover took %v", took)
	}

	// The new leader tracks liveness: a storage failure is still detected.
	c.KillNode(0)
	if err := c.WaitNodeOut(0, 10*time.Second); err != nil {
		t.Fatalf("new leader did not detect storage failure: %v", err)
	}

	// The old leader rejoins as a follower and catches up.
	if err := c.RestartMeta(old); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, "restarted node to catch up", func() bool {
		l := c.Leader()
		return l >= 0 && c.Metas[old].Raft().AppliedIndex() >= c.Metas[l].Raft().AppliedIndex() &&
			c.Metas[old].FSM().Epoch() == c.Metas[l].FSM().Epoch()
	})
}

// A planned handover (leadership transfer) avoids the election timeout.
func TestPlannedLeadershipTransfer(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 3})
	ctx := ctxT(t, time.Minute)
	if _, err := c.MetaClient.Propose(ctx); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	start := time.Now()
	if _, err := c.MetaClient.TransferLeadership(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.MetaClient.Propose(ctx); err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	if l := c.Leader(); l == old || l < 0 {
		t.Fatalf("leadership did not move (old=%d now=%d)", old, l)
	}
	t.Logf("planned transfer: writes committing on the new leader after %v", took.Round(time.Microsecond))
}

// The leader and a storage node die together. The new leader must still mark
// the storage node DOWN (after its grace window) and repair its chunks.
func TestRepairSurvivesLeaderCrash(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 7, ChunkSize: testChunk, DeadAfter: 500 * time.Millisecond})
	ctx := ctxT(t, time.Minute)
	for i := 0; i < 4; i++ {
		if _, err := c.Client.UploadBytes(ctx, fmt.Sprintf("r%d", i), randomBytes(12*testChunk)); err != nil {
			t.Fatal(err)
		}
	}
	c.KillMeta(c.Leader())
	c.KillNode(5)
	if err := c.WaitNodeOut(5, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "RF restored under new leader", func() bool {
		for i := 0; i < 4; i++ {
			for _, n := range replicaCounts(t, c, ctx, fmt.Sprintf("r%d", i)) {
				if n < 3 {
					return false
				}
			}
		}
		return true
	})
}

// Losing a majority of the control plane stops membership changes but not
// the data plane, which keeps routing by its cached ring.
func TestDataPlaneSurvivesControlPlaneOutage(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 4, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	c.KillMeta(0)
	c.KillMeta(1)
	c.KillMeta(2)
	for i := 0; i < 5; i++ {
		data := randomBytes(3 * testChunk)
		name := fmt.Sprintf("noquorum-%d", i)
		if _, err := c.Client.UploadBytes(ctx, name, data); err != nil {
			t.Fatalf("upload with control plane down: %v", err)
		}
		if got := download(t, c, ctx, name); !bytes.Equal(got, data) {
			t.Fatal("mismatch")
		}
	}
	pctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := c.MetaClient.Propose(pctx); err == nil {
		t.Fatal("control plane accepted a write with no members up")
	}
}
