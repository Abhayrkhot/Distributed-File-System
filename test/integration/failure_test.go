package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/cluster"
)

// Many clients upload and download at once, through different coordinators.
func TestConcurrentTransfers(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 8, ChunkSize: testChunk})
	ctx := ctxT(t, 2*time.Minute)
	const workers, perWorker = 24, 3
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				name := fmt.Sprintf("w%02d/f%d", w, i)
				data := randomBytes(testChunk*(1+(w+i)%7) + w*101)
				if _, err := c.Client.UploadBytes(ctx, name, data); err != nil {
					errs <- fmt.Errorf("upload %s: %w", name, err)
					return
				}
				var buf bytes.Buffer
				if _, err := c.Client.Download(ctx, name, &buf); err != nil {
					errs <- fmt.Errorf("download %s: %w", name, err)
					return
				}
				if !bytes.Equal(buf.Bytes(), data) {
					errs <- fmt.Errorf("%s: content mismatch", name)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	files, _ := c.Client.List(ctx, "w", 0)
	if len(files) != workers*perWorker {
		t.Fatalf("want %d files, got %d", workers*perWorker, len(files))
	}
}

// With RF=3, reads survive the loss of any two storage nodes, immediately
// (before the control plane has even noticed).
func TestReadsSurviveTwoNodeLoss(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk, DeadAfter: time.Hour})
	ctx := ctxT(t, time.Minute)
	data := randomBytes(60 * testChunk)
	if _, err := c.Client.UploadBytes(ctx, "big", data); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "RF=3", func() bool {
		for _, n := range replicaCounts(t, c, ctx, "big") {
			if n < 3 {
				return false
			}
		}
		return true
	})
	c.KillNode(1)
	c.KillNode(4)
	c.Client.SetNodes([]string{c.Nodes[0].Addr(), c.Nodes[2].Addr(), c.Nodes[3].Addr(), c.Nodes[5].Addr()})
	for i := 0; i < 3; i++ {
		if got := download(t, c, ctx, "big"); !bytes.Equal(got, data) {
			t.Fatal("content mismatch after node loss")
		}
	}
	fo := int64(0)
	for _, i := range []int{0, 2, 3, 5} {
		fo += c.Nodes[i].Coordinator().ReadFailover.Load()
	}
	if fo == 0 {
		t.Fatal("expected reads to fail over to secondary replicas")
	}
}

// Writes keep succeeding while a node is down but not yet detected (quorum
// W=2 of N=3, plus sloppy quorum when two preference replicas are gone).
func TestWritesDuringUndetectedFailure(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk, DeadAfter: time.Hour})
	ctx := ctxT(t, time.Minute)
	c.KillNode(2)
	c.KillNode(3)
	var live []string
	for _, i := range []int{0, 1, 4, 5} {
		live = append(live, c.Nodes[i].Addr())
	}
	c.Client.SetNodes(live)
	for i := 0; i < 10; i++ {
		data := randomBytes(8 * testChunk)
		name := fmt.Sprintf("during-%d", i)
		if _, err := c.Client.UploadBytes(ctx, name, data); err != nil {
			t.Fatalf("upload with 2 nodes down: %v", err)
		}
		if got := download(t, c, ctx, name); !bytes.Equal(got, data) {
			t.Fatal("mismatch")
		}
	}
	var sloppy int64
	for _, i := range []int{0, 1, 4, 5} {
		sloppy += c.Nodes[i].Coordinator().SloppyWrites.Load()
	}
	t.Logf("sloppy-quorum writes: %d", sloppy)
}

// Without a write quorum an upload fails and leaves no visible file.
func TestNoQuorumNoPartialFile(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 3, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	c.KillNode(1)
	c.KillNode(2)
	c.Client.SetNodes([]string{c.Nodes[0].Addr()})
	if _, err := c.Client.UploadBytes(ctx, "nope", randomBytes(5*testChunk)); err == nil {
		t.Fatal("upload succeeded without a write quorum")
	}
	if _, err := c.Client.Stat(ctx, "nope"); err == nil {
		t.Fatal("failed upload left a visible manifest")
	}
	// Still refused once the control plane has removed the dead nodes.
	if err := c.WaitRingSize(1, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Client.UploadBytes(ctx, "nope", randomBytes(testChunk)); err == nil {
		t.Fatal("upload succeeded on a 1-node ring with W=2")
	}
}

// Readers racing concurrent writers of one name always see one complete
// version, never a mix.
func TestConcurrentWritersSameName(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk})
	ctx := ctxT(t, 2*time.Minute)
	const writers = 8
	versions := make([][]byte, writers)
	valid := map[string]bool{}
	for i := range versions {
		versions[i] = randomBytes(testChunk*3 + i*977)
		valid[sum(versions[i])] = true
	}
	// Seed so readers never see NotFound.
	if _, err := c.Client.UploadBytes(ctx, "hot", versions[0]); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var reads, bad atomic.Int64
	var rwg sync.WaitGroup
	for r := 0; r < 6; r++ {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			for !stop.Load() {
				var buf bytes.Buffer
				if _, err := c.Client.Download(ctx, "hot", &buf); err != nil {
					t.Errorf("read during writes: %v", err)
					return
				}
				reads.Add(1)
				if !valid[sum(buf.Bytes())] {
					bad.Add(1)
				}
			}
		}()
	}
	var wwg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wwg.Add(1)
		go func(w int) {
			defer wwg.Done()
			for k := 0; k < 3; k++ {
				if _, err := c.Client.UploadBytes(ctx, "hot", versions[w]); err != nil {
					t.Errorf("writer %d: %v", w, err)
				}
			}
		}(w)
	}
	wwg.Wait()
	stop.Store(true)
	rwg.Wait()
	if bad.Load() > 0 {
		t.Fatalf("%d of %d reads returned a torn file", bad.Load(), reads.Load())
	}
	t.Logf("%d reads during %d concurrent writes, all consistent", reads.Load(), writers*3)
}

// A dead node is detected, the ring moves past it, and the leader restores
// RF=3 for every chunk it held, from surviving replicas.
func TestRepairAfterNodeFailure(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 8, ChunkSize: testChunk, DeadAfter: 500 * time.Millisecond})
	ctx := ctxT(t, time.Minute)
	for i := 0; i < 6; i++ {
		if _, err := c.Client.UploadBytes(ctx, fmt.Sprintf("f%d", i), randomBytes(16*testChunk)); err != nil {
			t.Fatal(err)
		}
	}
	victim := 3
	held, _ := c.Nodes[victim].Store().Stats()
	t0 := time.Now()
	c.KillNode(victim)
	if err := c.WaitNodeOut(victim, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	detected := time.Since(t0)
	// Repair is triggered by the epoch change; wait until every chunk is back
	// to three replicas on the new ring.
	restored := waitFor(t, 20*time.Second, "RF restored", func() bool {
		for i := 0; i < 6; i++ {
			for _, n := range replicaCounts(t, c, ctx, fmt.Sprintf("f%d", i)) {
				if n < 3 {
					return false
				}
			}
		}
		return true
	})
	st, _ := c.MetaClient.LeaderStatus(ctx)
	t.Logf("victim held %d chunks; DOWN after %v; RF=3 restored %v later; repair: %+v",
		held, detected.Round(time.Millisecond), restored.Round(time.Millisecond), st.Repair)
}

// When every replica of a chunk is lost (disks wiped), the leader restores it
// from the S3 tier.
func TestRestoreFromS3AfterTotalReplicaLoss(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 6, ChunkSize: testChunk, DeadAfter: 500 * time.Millisecond})
	ctx := ctxT(t, 2*time.Minute)
	data := randomBytes(30 * testChunk)
	if _, err := c.Client.UploadBytes(ctx, "precious", data); err != nil {
		t.Fatal(err)
	}
	f, _ := c.Metastore().GetFile(ctx, "precious", "")
	waitFor(t, 15*time.Second, "S3 write-behind to finish", func() bool {
		for _, ch := range f.Chunks {
			if ok, _ := c.Blob().Exists(ctx, blob.ChunkKey(ch.ID)); !ok {
				return false
			}
		}
		return true
	})
	// Destroy all three replicas of the first chunk.
	victims := map[int]bool{}
	for _, m := range c.LeaderRing().Lookup(f.Chunks[0].ID, 3) {
		victims[c.NodeByID(m.ID)] = true
	}
	for v := range victims {
		c.KillNode(v)
		if err := c.WipeNode(v); err != nil {
			t.Fatal(err)
		}
	}
	var live []string
	for i := range c.Nodes {
		if !victims[i] {
			live = append(live, c.Nodes[i].Addr())
		}
	}
	c.Client.SetNodes(live)
	if err := c.WaitRingSize(3, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 30*time.Second, "chunks restored", func() bool {
		for _, n := range replicaCounts(t, c, ctx, "precious") {
			if n < 3 {
				return false
			}
		}
		return true
	})
	st, _ := c.MetaClient.LeaderStatus(ctx)
	if got := download(t, c, ctx, "precious"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch after S3 restore")
	}
	t.Logf("repair after losing 3 of 6 nodes: %+v", st.Repair)
}

// A node that comes back is re-admitted and back-filled with what it missed.
func TestNodeRejoin(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 5, ChunkSize: testChunk, DeadAfter: 400 * time.Millisecond})
	ctx := ctxT(t, time.Minute)
	if _, err := c.Client.UploadBytes(ctx, "before", randomBytes(20*testChunk)); err != nil {
		t.Fatal(err)
	}
	c.KillNode(2)
	if err := c.WaitNodeOut(2, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	live := []string{c.Nodes[0].Addr(), c.Nodes[1].Addr(), c.Nodes[3].Addr(), c.Nodes[4].Addr()}
	c.Client.SetNodes(live)
	if _, err := c.Client.UploadBytes(ctx, "while-down", randomBytes(20*testChunk)); err != nil {
		t.Fatal(err)
	}
	if err := c.RestartNode(2); err != nil {
		t.Fatal(err)
	}
	if err := c.WaitRingSize(5, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "rejoined node back-filled", func() bool {
		for _, name := range []string{"before", "while-down"} {
			for _, n := range replicaCounts(t, c, ctx, name) {
				if n < 3 {
					return false
				}
			}
		}
		return true
	})
}

// A replica whose bytes rotted on disk is detected by hash and skipped.
func TestCorruptReplicaIsSkipped(t *testing.T) {
	c := startCluster(t, cluster.Options{Storage: 4, ChunkSize: testChunk})
	ctx := ctxT(t, time.Minute)
	data := randomBytes(4 * testChunk)
	if _, err := c.Client.UploadBytes(ctx, "rot", data); err != nil {
		t.Fatal(err)
	}
	f, _ := c.Metastore().GetFile(ctx, "rot", "")
	waitFor(t, 5*time.Second, "RF=3", func() bool { return len(c.Holders(f.Chunks[0].ID)) == 3 })
	// Corrupt every chunk on the primary of chunk 0.
	primary := c.NodeByID(c.LeaderRing().Lookup(f.Chunks[0].ID, 1)[0].ID)
	err := c.Nodes[primary].Store().Walk(func(id string, _ int64) error {
		p := filepath.Join(c.Nodes[primary].Store().Root(), "chunks", id[0:2], id[2:4], id)
		b, _ := os.ReadFile(p)
		b[0] ^= 0xff
		return os.WriteFile(p, b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range c.Nodes {
		c.Client.SetNodes([]string{n.Addr()}) // every node as coordinator
		if got := download(t, c, ctx, "rot"); !bytes.Equal(got, data) {
			t.Fatal("served corrupt data")
		}
	}
}

func init() { _ = context.Background }

// Regression: after the ring grows (or nodes flap DOWN and back), chunks sit
// outside their new preference lists until repair moves them. Reads must
// still find them, by asking every known node, and repair must then put
// every chunk back on its preference list.
func TestReadsDuringRingChurnBeforeRepair(t *testing.T) {
	// No S3 tier: the copies on the original nodes are the only ones.
	c := startCluster(t, cluster.Options{Storage: 4, ChunkSize: testChunk, ManualRepair: true, NoBlob: true})
	ctx := ctxT(t, time.Minute)
	data := randomBytes(40 * testChunk)
	if _, err := c.Client.UploadBytes(ctx, "churn", data); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 12; i++ {
		if _, err := c.AddNode(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.WaitRingSize(16, 15*time.Second); err != nil {
		t.Fatal(err)
	}
	// With 16 nodes, most chunks' first five ring candidates are new nodes
	// that hold nothing yet.
	misplaced := 0
	for _, n := range replicaCounts(t, c, ctx, "churn") {
		if n == 0 {
			misplaced++
		}
	}
	if misplaced == 0 {
		t.Fatal("test setup: expected chunks with no replica on their new preference list")
	}
	var nodes []string
	for _, n := range c.Nodes {
		nodes = append(nodes, n.Addr())
	}
	c.Client.SetNodes(nodes)
	if got := download(t, c, ctx, "churn"); !bytes.Equal(got, data) {
		t.Fatal("content mismatch")
	}
	if err := c.Repair(ctx); err != nil {
		t.Fatal(err)
	}
	for _, n := range replicaCounts(t, c, ctx, "churn") {
		if n != 3 {
			t.Fatalf("after repair a chunk has %d replicas on its preference list", n)
		}
	}
	t.Logf("%d of 40 chunks had no replica on their new preference list; read OK; repair restored all", misplaced)
}
