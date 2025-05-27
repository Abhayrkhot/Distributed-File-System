# Results

All numbers come from `scripts/run-benchmarks.sh`. The raw JSON (including
per-file latency distributions and CPU samples) is in `bench/results/`, and
`scripts/results-table.py` renders the tables below from it.

## Setup

| | |
|---|---|
| Host | One Apple M4 laptop (10 cores, 16 GB RAM, internal SSD), macOS, Go 1.27 |
| Cluster | 3 `dfs-meta` + 50 or 64 `dfs-storage` OS processes on loopback, `GOMAXPROCS=2` each |
| Metadata | Cassandra 5.0.9, one native node, 1 GB heap, `QUORUM` (= 1 replica here) |
| S3 tier | MinIO, native, AES-256-GCM objects, 1 write-behind worker per node |
| Data | 96 files × 16 MiB of random bytes (1.5 GiB) from 32 concurrent client streams, then 30 s of random reads by the same 32 streams |
| Replication | 4 MiB chunks, N=3, W=2; Raft log fsync on, chunk fsync off |
| Verification | every upload's server checksum compared with the client's; every download SHA-256-verified |

**What "single host" means for these numbers.** Every storage node,
Cassandra, MinIO and the client share one SSD and talk over loopback. One
uploaded byte crosses loopback four times and is written to disk four times
(three replicas plus the S3 copy). The physical write rate is therefore about
4× the reported upload MB/s. The numbers exercise the protocols, concurrency
and failure handling at 50+ nodes. They are not a network benchmark.

**Why there are three runs per size.** On this machine the limiting resource
is the SSD's writeback. With 50 concurrent writers, `close()` on a freshly
written 4 MiB file has a p99 of 1–2 s on APFS, and a standalone
microbenchmark of the same write pattern swings between 650 and 2,000 MB/s
from run to run. Instrumentation inside the nodes showed slow uploads waiting
almost entirely on that step ("store_and_hash" in the logs), not on the
network, Cassandra or the client. So the tables report the median with the
range.

<!-- TABLES -->

## Reading the Raft numbers

- **Baseline → final.** Stock hashicorp/raft settings (1 s heartbeat timeout,
  BoltDB) take about 2.3 s to restore writes after a leader crash. LAN timing
  alone brings that to about 0.5 s. Crash detection from connection loss
  halves it again, because a follower learns the leader died from the
  kernel's FIN instead of waiting out a randomized 200–400 ms heartbeat
  timeout.
- **The WAL store matters less at this heartbeat timeout than at 100 ms.** A
  vote costs one `F_FULLFSYNC` (5–8 ms here) instead of two. With the
  aggressive 100 ms profile, the store change alone moved p50 from 307 ms to
  224 ms in an earlier run. At 200 ms the gain (about 25 ms in the mean) is
  within the timeout's randomization.
- **Freeze** (SIGSTOP: the leader hangs, its sockets stay open) gets no
  connection-loss signal. Detection falls back to the 200–400 ms heartbeat
  timeout, which is the worst case for a leader that is alive but stuck.
- **Graceful** (SIGTERM) hands leadership over before exiting, which is what a
  rolling restart in Kubernetes does.
- **Aggressive** recovers faster when idle, but it is the profile that had
  spurious elections under load (below), which is why it isn't the default.

## Control plane under load

Leader elections during the throughput runs were counted from the Raft term
before and after. The data plane does not depend on the leader: transfers
continued with no errors while they happened. But elections are still a
cost, since membership changes and repair wait for a leader.

The benchmark also runs a stall probe: a 5 ms sleep in the client process,
recording how late it wakes. On this host under full load it wakes up to
several hundred ms late. A Raft heartbeat timeout has to sit above the stalls
its hosts actually see. That is why the default profile uses 200 ms and not
100 ms, and why some elections still happen when stalls exceed even that. On
dedicated control-plane hosts, as in the Kubernetes manifests, the members
don't compete with 50 storage processes for one CPU and disk.

## How the numbers changed during development

The first 50-process run uploaded at 5 MB/s, failed reads, and ran 30
elections. What each finding fixed:

| Finding | Fix |
|---|---|
| Uploads stalled 13 s: gocql dialled hosts it discovered on its default port 9042, not the configured one | Set `ClusterConfig.Port` from the contact points |
| Committed files unreadable after membership churn: chunks sat outside the 5 nodes the new ring pointed at | Reads locate holders on every known node before trying S3 (`TestReadsDuringRingChurnBeforeRepair`) |
| Storage nodes burned 7–8 cores to move 20 MB/s: 53 processes each with `GOMAXPROCS=10` spun in the Go scheduler | `GOMAXPROCS=2` per process (what Go 1.25+ picks for a 2-CPU pod); upload 20 → 91 MB/s in the same run |
| Election storms (term 2 → 23 in one run): a 100 ms election timeout was shorter than a vote's fsync under load | 1 s election timeout (free on success), 200 ms heartbeat |
| A third of failovers took the slow path: an old restore timer reset the crash-detect window mid-failover | Generation counter on the window; p90 499 → ~285 ms |
| Duplicate S3 uploads after elections | Write-behind skips chunks repair already uploaded |

## DHT routing (unit tests)

From `internal/ring` with 128 virtual nodes:

- 50 nodes, 200k chunks at RF=3: per-node load coefficient of variation 5.6%.
- Removing 1 of 50 nodes changed 5.65% of replica sets (3/50 = 6% expected),
  and no replica set that excluded the removed node.
- `Lookup` takes 0.4 µs for a 3-replica preference list on a 50-node ring.

## Reproduce

```bash
scripts/run-benchmarks.sh
```

```bash
python3 scripts/results-table.py
```
