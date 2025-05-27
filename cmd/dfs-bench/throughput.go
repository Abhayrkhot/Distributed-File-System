package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"hash"
	"io"
	mrand "math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dfsv1 "github.com/Abhayrkhot/Distributed-File-System/gen/dfs/v1"
	"github.com/Abhayrkhot/Distributed-File-System/internal/blob"
	"github.com/Abhayrkhot/Distributed-File-System/internal/client"
	"github.com/Abhayrkhot/Distributed-File-System/internal/localcluster"
	"github.com/Abhayrkhot/Distributed-File-System/internal/meta"
	"github.com/Abhayrkhot/Distributed-File-System/internal/metastore"
	"github.com/Abhayrkhot/Distributed-File-System/internal/rpcutil"
)

type PhaseResult struct {
	Files      int     `json:"files"`
	Bytes      int64   `json:"bytes"`
	Seconds    float64 `json:"seconds"`
	MBps       float64 `json:"mb_per_s"`  // 1 MB = 1e6 bytes of file data
	MiBps      float64 `json:"mib_per_s"` // 1 MiB = 2^20 bytes
	Errors     int64   `json:"errors"`
	Retries    int64   `json:"retries"`
	Mismatches int64   `json:"checksum_mismatches"`
	LatencyMs  Dist    `json:"per_file_latency_ms"`
}

type RepairResult struct {
	Victim           string  `json:"victim"`
	VictimChunks     int64   `json:"victim_chunks"`
	VictimBytes      int64   `json:"victim_bytes"`
	DetectMs         float64 `json:"detect_ms"`      // SIGKILL -> node DOWN in the ring
	RepairPassMs     float64 `json:"repair_pass_ms"` // duration of the pass that repaired it
	RestoredMs       float64 `json:"restored_ms"`    // SIGKILL -> repair pass finished
	ReplicasRepaired int64   `json:"replicas_repaired"`
	VerifyUnderRepl  int64   `json:"verify_under_replicated"` // must be 0
	VerifyScanned    int64   `json:"verify_chunks_scanned"`
	ReadsAfterRepair int     `json:"files_read_back_after_repair"`
	ReadErrorsAfter  int64   `json:"read_errors_after_repair"`
}

type ThroughputResult struct {
	Env            Env           `json:"env"`
	MetaNodes      int           `json:"meta_nodes"`
	StorageNodes   int           `json:"storage_nodes"`
	Clients        int           `json:"clients"`
	FileSize       int64         `json:"file_size"`
	ChunkSize      int64         `json:"chunk_size"`
	Replicas       int           `json:"replicas"`
	WriteQuorum    int           `json:"write_quorum"`
	Fsync          bool          `json:"chunk_fsync"`
	Metastore      string        `json:"metastore"`
	ColdTier       string        `json:"cold_tier"`
	Upload         PhaseResult   `json:"upload"`
	S3DrainMs      float64       `json:"s3_writebehind_drain_ms"`
	S3BackupsDone  int64         `json:"s3_backups_done"`
	Download       PhaseResult   `json:"download"`
	Repair         *RepairResult `json:"repair,omitempty"`
	StorageRSSMiB  float64       `json:"storage_nodes_total_rss_mib"`
	RaftTermStart  uint64        `json:"raft_term_start"`
	RaftTermEnd    uint64        `json:"raft_term_end"` // before the repair phase; equal means no elections under load
	RingEpochStart uint64        `json:"ring_epoch_start"`
	RingEpochEnd   uint64        `json:"ring_epoch_end"` // before the repair phase; equal means no node was marked DOWN under load
	GOMAXPROCS     int           `json:"child_gomaxprocs"`
	MetaArgs       string        `json:"extra_meta_args,omitempty"`
	BackupWorkers  int           `json:"s3_backup_workers_per_node"`
	CPUSamples     []CPUSample   `json:"cpu_samples,omitempty"`
	Notes          []string      `json:"notes"`
}

// genReader yields n pseudo-random bytes from a seed and hashes them.
type genReader struct {
	src  *mrand.ChaCha8
	left int64
	h    hash.Hash
}

func newGen(seed uint64, n int64) *genReader {
	var s [32]byte
	for i := 0; i < 4; i++ {
		v := seed + uint64(i)*0x9e3779b97f4a7c15
		for j := 0; j < 8; j++ {
			s[i*8+j] = byte(v >> (8 * j))
		}
	}
	return &genReader{src: mrand.NewChaCha8(s), left: n, h: sha256.New()}
}

func (g *genReader) Read(p []byte) (int, error) {
	if g.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > g.left {
		p = p[:g.left]
	}
	g.src.Read(p)
	g.h.Write(p)
	g.left -= int64(len(p))
	return len(p), nil
}

type hashWriter struct {
	h hash.Hash
	n int64
}

func (w *hashWriter) Write(p []byte) (int, error) {
	w.h.Write(p)
	w.n += int64(len(p))
	return len(p), nil
}

func runThroughput(args []string) error {
	fs := flag.NewFlagSet("throughput", flag.ExitOnError)
	storageN := fs.Int("storage", 50, "storage node processes")
	metaN := fs.Int("meta", 3, "control-plane processes")
	clients := fs.Int("clients", 32, "concurrent client streams")
	fileSizeS := fs.String("file-size", "16MiB", "size of each file")
	totalS := fs.String("total", "1536MiB", "total bytes to upload")
	chunkS := fs.String("chunk-size", "4MiB", "chunk size")
	dlSecs := fs.Duration("download-for", 30*time.Second, "how long to run the download phase")
	fsync := fs.Bool("fsync", false, "fsync each chunk on the storage nodes")
	doRepair := fs.Bool("repair", true, "kill a storage node at the end and measure repair")
	out := fs.String("out", "bench/results", "results directory")
	bin := fs.String("bin", repoBin(), "directory with dfs-meta and dfs-storage")
	label := fs.String("label", "", "suffix for the result file name")
	keep := fs.Bool("keep", false, "keep the cluster's data and logs directory")
	procs := fs.Int("gomaxprocs", 2, "GOMAXPROCS for each dfs-meta/dfs-storage process (0: Go default, all cores). "+
		"53 processes each defaulting to every core spend their CPU in scheduler spinning; 2 is what Go picks for a pod with a 2-CPU limit")
	extraMeta := fs.String("meta-args", "", "extra dfs-meta flags, space separated")
	backupWorkers := fs.Int("backup-workers", 4, "S3 write-behind workers per storage node")
	raftLog := fs.String("raft-log-level", "warn", "dfs-meta raft log level")
	fs.Parse(args)

	fileSize, err := parseSize(*fileSizeS)
	if err != nil {
		return err
	}
	total, err := parseSize(*totalS)
	if err != nil {
		return err
	}
	chunk, err := parseSize(*chunkS)
	if err != nil {
		return err
	}
	cassHosts := os.Getenv("DFS_CASSANDRA")
	if cassHosts == "" {
		return fmt.Errorf("DFS_CASSANDRA must point at Cassandra (scripts/dev-deps.sh up); the processes share one metastore")
	}
	ctx := context.Background()
	stamp := time.Now().Unix()
	keyspace := fmt.Sprintf("dfs_bench_%d", stamp)
	bucket := fmt.Sprintf("dfs-bench-%d", stamp)
	encKey := make([]byte, 32)
	rand.Read(encKey)

	// Create the schema once, before 50 processes race to do it.
	cs, err := metastore.NewCassandra(ctx, metastore.CassandraConfig{Hosts: strings.Split(cassHosts, ","), Keyspace: keyspace})
	if err != nil {
		return err
	}
	defer func() { cs.DropKeyspace(context.Background()); cs.Close() }()

	res := ThroughputResult{GOMAXPROCS: *procs, MetaArgs: *extraMeta, BackupWorkers: *backupWorkers, Env: hostEnv(), MetaNodes: *metaN, StorageNodes: *storageN, Clients: *clients, FileSize: fileSize,
		ChunkSize: chunk, Replicas: 3, WriteQuorum: 2, Fsync: *fsync, Metastore: "cassandra " + cassHosts}
	env := []string{"DFS_KEYSPACE=" + keyspace, "DFS_S3_BUCKET=" + bucket, "DFS_S3_ENCRYPTION_KEY=" + hex.EncodeToString(encKey)}
	if *procs > 0 {
		env = append(env, fmt.Sprintf("GOMAXPROCS=%d", *procs))
	}
	var s3 *blob.S3
	if cfg, ok := blob.S3ConfigFromEnv(); ok {
		cfg.Bucket, cfg.CreateBucket = bucket, true
		if s3, err = blob.NewS3(ctx, cfg); err != nil {
			return err
		}
		res.ColdTier = "s3 (MinIO) " + cfg.Endpoint + ", AES-256-GCM"
	} else {
		res.ColdTier = "none (DFS_S3_ENDPOINT unset)"
	}

	dir, err := os.MkdirTemp("", "dfs-throughput-*.noindex") // .noindex: skipped by Spotlight
	if err != nil {
		return err
	}
	if *keep {
		fmt.Println("keeping", dir)
	} else {
		defer os.RemoveAll(dir)
	}
	storageArgs := []string{"--log-level", "warn", "--chunk-size", fmt.Sprint(chunk), "--backup-workers", fmt.Sprint(*backupWorkers)}
	if *fsync {
		storageArgs = append(storageArgs, "--fsync")
	}
	fmt.Printf("starting %d meta + %d storage processes ...\n", *metaN, *storageN)
	start := time.Now()
	c, err := localcluster.Start(ctx, localcluster.Options{
		BinDir: *bin, Dir: dir, Meta: *metaN, Storage: *storageN, Env: env,
		MetaArgs: append([]string{"--timing", "fast", "--dead-after", "3s", "--repair-interval", "1h", "--log-level", "info", "--raft-log-level", *raftLog},
			strings.Fields(*extraMeta)...),
		StorageArgs: storageArgs,
	})
	if err != nil {
		return err
	}
	defer c.Stop()
	fmt.Printf("cluster up in %v\n", time.Since(start).Round(time.Millisecond))
	var addrs []string
	for _, n := range c.Nodes {
		addrs = append(addrs, n.Addr)
	}
	cl := client.New(addrs)
	defer cl.Close()
	mc := meta.NewClient(c.MetaAddrs, nil)
	defer mc.Close()

	if st, err := mc.LeaderStatus(ctx); err == nil {
		res.RaftTermStart, res.RingEpochStart = st.Term, st.Ring.Epoch
	}

	// ---- upload ----
	nFiles := int(total / fileSize)
	type file = benchFile
	files := make([]file, nFiles)
	jobs := make(chan int, nFiles)
	for i := 0; i < nFiles; i++ {
		jobs <- i
	}
	close(jobs)
	var upBytes, upErrs, upRetries atomic.Int64
	var mu sync.Mutex
	var upLat []float64
	stopSampler := sampleCPU(&res.CPUSamples, 5*time.Second)
	fmt.Printf("upload: %d files x %s from %d clients\n", nFiles, *fileSizeS, *clients)
	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				name := fmt.Sprintf("bench/%06d.bin", i)
				var (
					g   *genReader
					fi  *dfsv1.FileInfo
					err error
				)
				fs := time.Now()
				for attempt := 0; attempt < 3; attempt++ {
					if attempt > 0 {
						upRetries.Add(1)
						fmt.Fprintf(os.Stderr, "upload %s: retrying on another node after: %v\n", name, err)
					}
					g = newGen(uint64(stamp)*1_000_003+uint64(i), fileSize)
					fi, err = cl.UploadTo(ctx, cl.Pick(), name, g)
					if err == nil || !rpcutil.IsUnavailable(err) {
						break
					}
				}
				lat := time.Since(fs)
				if err != nil {
					upErrs.Add(1)
					fmt.Fprintf(os.Stderr, "upload %s: %v\n", name, err)
					continue
				}
				want := hex.EncodeToString(g.h.Sum(nil))
				if fi.Sha256 != want {
					upErrs.Add(1)
					fmt.Fprintf(os.Stderr, "upload %s: server checksum %s != client %s\n", name, fi.Sha256, want)
				}
				files[i] = file{name: name, sha: want}
				upBytes.Add(fi.Size)
				mu.Lock()
				upLat = append(upLat, ms(lat))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	upDur := time.Since(t0)
	res.Upload = phase(len(upLat), upBytes.Load(), upDur, upErrs.Load(), 0, upLat)
	res.Upload.Retries = upRetries.Load()
	fmt.Printf("upload: %.1f MB/s (%.1f MiB/s) over %v, %d errors, per-file p50 %.0f ms p99 %.0f ms\n",
		res.Upload.MBps, res.Upload.MiBps, upDur.Round(time.Millisecond), res.Upload.Errors, res.Upload.LatencyMs.P50, res.Upload.LatencyMs.P99)

	// ---- S3 write-behind drain ----
	pool := rpcutil.NewPool()
	defer pool.Close()
	nodeStatus := func() (queue, done int64) {
		for _, n := range c.Nodes {
			if !n.Running() {
				continue
			}
			conn, err := pool.Get(n.Addr)
			if err != nil {
				continue
			}
			sctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			st, err := dfsv1.NewChunkServiceClient(conn).NodeStatus(sctx, &dfsv1.NodeStatusRequest{})
			cancel()
			if err == nil {
				queue += st.BackupQueueDepth + st.BackupsInflight
				done += st.BackupsDone
			}
		}
		return
	}
	if s3 != nil {
		d0 := time.Now()
		zeros := 0
		for {
			q, done := nodeStatus()
			if q == 0 {
				zeros++
			} else {
				zeros = 0
			}
			if zeros == 3 { // stable: nothing queued or uploading for 3 polls
				res.S3DrainMs = ms(time.Since(d0))
				res.S3BackupsDone = done
				break
			}
			if time.Since(d0) > 5*time.Minute {
				res.Notes = append(res.Notes, fmt.Sprintf("S3 queue did not drain within 5 min (%d left)", q))
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Printf("s3 write-behind drained %v after upload finished (%d chunk objects)\n",
			time.Duration(res.S3DrainMs*float64(time.Millisecond)).Round(time.Millisecond), res.S3BackupsDone)
	}

	var rss int64
	for _, n := range c.Nodes {
		rss += n.RSS()
	}
	res.StorageRSSMiB = float64(rss) / 1024

	// ---- download ----
	var good []file
	for _, f := range files {
		if f.name != "" {
			good = append(good, f)
		}
	}
	var dlBytes, dlErrs, dlBad atomic.Int64
	var dlLat []float64
	fmt.Printf("download: random files for %v from %d clients\n", *dlSecs, *clients)
	t1 := time.Now()
	deadline := t1.Add(*dlSecs)
	for w := 0; w < *clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := mrand.New(mrand.NewPCG(uint64(w), uint64(stamp)))
			for time.Now().Before(deadline) {
				f := good[r.IntN(len(good))]
				hw := &hashWriter{h: sha256.New()}
				fs := time.Now()
				_, err := cl.DownloadFrom(ctx, cl.Pick(), f.name, "", hw)
				lat := time.Since(fs)
				if err != nil {
					dlErrs.Add(1)
					fmt.Fprintf(os.Stderr, "download %s: %v\n", f.name, err)
					continue
				}
				if hex.EncodeToString(hw.h.Sum(nil)) != f.sha {
					dlBad.Add(1)
				}
				dlBytes.Add(hw.n)
				mu.Lock()
				dlLat = append(dlLat, ms(lat))
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	dlDur := time.Since(t1)
	res.Download = phase(len(dlLat), dlBytes.Load(), dlDur, dlErrs.Load(), dlBad.Load(), dlLat)
	fmt.Printf("download: %.1f MB/s (%.1f MiB/s) over %v, %d files, %d errors, %d checksum mismatches, per-file p50 %.0f ms p99 %.0f ms\n",
		res.Download.MBps, res.Download.MiBps, dlDur.Round(time.Millisecond), res.Download.Files, res.Download.Errors,
		res.Download.Mismatches, res.Download.LatencyMs.P50, res.Download.LatencyMs.P99)

	stopSampler()
	if st, err := mc.LeaderStatus(ctx); err == nil {
		res.RaftTermEnd, res.RingEpochEnd = st.Term, st.Ring.Epoch
	}

	// ---- repair after a storage-node crash ----
	if *doRepair {
		rr, err := measureRepair(ctx, c, mc, pool, cl, good)
		if err != nil {
			res.Notes = append(res.Notes, "repair phase: "+err.Error())
			fmt.Println("repair phase failed:", err)
		} else {
			res.Repair = rr
			fmt.Printf("repair: %s (%d chunks, %.0f MiB) DOWN after %.0f ms; pass of %.0f ms re-replicated %d chunks; RF restored %.0f ms after the kill; verify pass: %d under-replicated of %d\n",
				rr.Victim, rr.VictimChunks, float64(rr.VictimBytes)/(1<<20), rr.DetectMs, rr.RepairPassMs, rr.ReplicasRepaired, rr.RestoredMs, rr.VerifyUnderRepl, rr.VerifyScanned)
		}
	}

	res.Notes = append(res.Notes,
		"All processes, Cassandra and MinIO run on one host and talk over loopback; MB/s counts file bytes, each of which is written to 3 replicas (plus 1 S3 copy).",
		"Download throughput includes SHA-256 verification of every file on the client.")
	name := "throughput"
	if *label != "" {
		name += "-" + *label
	}
	p, err := writeJSON(*out, name+".json", res)
	if err != nil {
		return err
	}
	fmt.Println("wrote", p)

	if s3 != nil {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		s3.RemoveAll(cctx)
		cancel()
	}
	return nil
}

func phase(files int, bytes int64, d time.Duration, errs, bad int64, lat []float64) PhaseResult {
	sec := d.Seconds()
	return PhaseResult{Files: files, Bytes: bytes, Seconds: sec, MBps: float64(bytes) / 1e6 / sec,
		MiBps: float64(bytes) / (1 << 20) / sec, Errors: errs, Mismatches: bad, LatencyMs: dist(lat)}
}

type benchFile struct {
	name string
	sha  string
}

func measureRepair(ctx context.Context, c *localcluster.Cluster, mc *meta.Client, pool *rpcutil.Pool, cl *client.Client, files []benchFile) (*RepairResult, error) {
	victim := c.Nodes[len(c.Nodes)/2]
	conn, err := pool.Get(victim.Addr)
	if err != nil {
		return nil, err
	}
	st, err := dfsv1.NewChunkServiceClient(conn).NodeStatus(ctx, &dfsv1.NodeStatusRequest{})
	if err != nil {
		return nil, err
	}
	rr := &RepairResult{Victim: victim.Name, VictimChunks: st.Chunks, VictimBytes: st.Bytes}
	before, err := mc.LeaderStatus(ctx)
	if err != nil {
		return nil, err
	}
	runsBefore := before.Repair.Runs

	t0 := time.Now()
	if err := victim.Kill(); err != nil {
		return nil, err
	}
	// Detection: the victim leaves the ring.
	var downEpoch uint64
	for {
		r, rp, err := mc.GetRing(ctx)
		if err == nil {
			if _, in := r.Member(victim.Name); !in {
				rr.DetectMs = ms(time.Since(t0))
				downEpoch = rp.Epoch
				break
			}
		}
		if time.Since(t0) > time.Minute {
			return nil, fmt.Errorf("victim never marked DOWN")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The epoch change triggers a pass (after a short debounce); wait for a
	// completed pass at or after the DOWN epoch.
	for {
		s, err := mc.LeaderStatus(ctx)
		if err == nil && s.Repair.Runs > runsBefore && !s.Repair.Running && s.Repair.LastEpoch >= downEpoch {
			rr.RepairPassMs = float64(s.Repair.LastDurationMs)
			rr.ReplicasRepaired = s.Repair.ReplicasRepaired
			rr.RestoredMs = float64(s.Repair.LastStartedUnixMs + s.Repair.LastDurationMs - t0.UnixMilli())
			break
		}
		if time.Since(t0) > 5*time.Minute {
			return nil, fmt.Errorf("no repair pass completed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Verification pass: everything should already be at RF=3.
	v, err := mc.TriggerRepair(ctx, true)
	if err != nil {
		return nil, err
	}
	rr.VerifyUnderRepl, rr.VerifyScanned = v.UnderReplicated, v.ChunksScanned
	// Read a sample back with the victim gone.
	var live []string
	for _, n := range c.Nodes {
		if n.Running() {
			live = append(live, n.Addr)
		}
	}
	cl.SetNodes(live)
	for i := 0; i < len(files) && i < 40; i++ {
		f := files[(i*7919)%len(files)]
		hw := &hashWriter{h: sha256.New()}
		if _, err := cl.Download(ctx, f.name, hw); err != nil || hex.EncodeToString(hw.h.Sum(nil)) != f.sha {
			rr.ReadErrorsAfter++
		}
		rr.ReadsAfterRepair++
	}
	return rr, nil
}

// CPUSample is host CPU use by process group, in percent of one core.
type CPUSample struct {
	AtSec   float64            `json:"at_s"`
	ByGroup map[string]float64 `json:"by_group"`
	// MaxTimerLateMs is the worst wake-up delay of a 5 ms sleep in the
	// benchmark process over the interval: a proxy for OS scheduling stalls
	// that every process on the host (including the Raft members) also sees.
	MaxTimerLateMs float64 `json:"max_timer_late_ms"`
}

// sampleCPU records `ps` CPU use grouped by program every interval until the
// returned stop function is called, and prints each sample.
func sampleCPU(out *[]CPUSample, every time.Duration) func() {
	start := time.Now()
	done := make(chan struct{})
	finished := make(chan struct{})
	var lateMu sync.Mutex
	var maxLate time.Duration
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			t0 := time.Now()
			time.Sleep(5 * time.Millisecond)
			late := time.Since(t0) - 5*time.Millisecond
			lateMu.Lock()
			maxLate = max(maxLate, late)
			lateMu.Unlock()
		}
	}()
	go func() {
		defer close(finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
			}
			raw, err := runCmd("ps", "-axo", "%cpu=,comm=")
			if err != nil {
				continue
			}
			g := map[string]float64{}
			for _, line := range strings.Split(raw, "\n") {
				f := strings.Fields(line)
				if len(f) < 2 {
					continue
				}
				var pct float64
				fmt.Sscan(f[0], &pct)
				name := strings.Join(f[1:], " ")
				if i := strings.LastIndexByte(name, '/'); i >= 0 {
					name = name[i+1:]
				}
				switch {
				case strings.HasPrefix(name, "dfs-"), name == "java", name == "minio":
				case strings.HasPrefix(name, "mds"), strings.HasPrefix(name, "mdworker"):
					name = "spotlight"
				case strings.Contains(name, "Virtualization"):
					name = "docker-vm"
				default:
					name = "other"
				}
				g[name] += pct
			}
			lateMu.Lock()
			late := maxLate
			maxLate = 0
			lateMu.Unlock()
			smp := CPUSample{AtSec: time.Since(start).Seconds(), ByGroup: g, MaxTimerLateMs: ms(late)}
			*out = append(*out, smp)
			fmt.Printf("  cpu@%3.0fs: timer-late max %5.1fms  storage %.0f%%  meta %.0f%%  bench %.0f%%  cassandra %.0f%%  minio %.0f%%  spotlight %.0f%%  docker-vm %.0f%%  other %.0f%%\n",
				smp.AtSec, smp.MaxTimerLateMs, g["dfs-storage"], g["dfs-meta"], g["dfs-bench"], g["java"], g["minio"], g["spotlight"], g["docker-vm"], g["other"])
		}
	}()
	return func() { close(done); <-finished }
}
