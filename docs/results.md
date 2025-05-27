# Results

Two environments:

- **60 machines on Explorer** (Northeastern's HPC cluster, Slurm). Every
  storage node, Raft member and the backends ran on its own machine, so all
  traffic crossed the real cluster network. Throughput, consistency and
  repair numbers come from here.
- **One Linux host** (20 cores, 61 GB RAM). The Raft failover matrix ran here:
  3 `dfs-meta` processes plus 10 heartbeating storage nodes, 30 leader kills
  per configuration.

The raw JSON (per-file latency distributions, CPU samples, scheduling-stall
probe) is in `bench/results-hpc/` and `bench/results/`.
`scripts/results-table.py` renders the tables below from it.

## 60 machines on Explorer

### Setup

| | |
|---|---|
| Machines | 64 distinct Explorer compute nodes: 60 × `dfs-storage`, 3 × `dfs-meta`, 1 × Cassandra + MinIO (Apptainer) |
| How | 32 two-node Slurm jobs (the `short` partition caps jobs at 2 nodes), submitted one at a time with `--exclude` so no two roles share a machine; jobs rendezvous through files on `/scratch` (`deploy/slurm/`) |
| Client | `dfs-bench` on one machine (Xeon Platinum 8276, 112 CPUs): 64 concurrent streams, 192 files × 16 MiB (3 GiB) uploaded, then 30 s of random downloads |
| Replication | 4 MiB chunks, N=3, W=2; chunk data on each node's local disk; write-behind to S3 (MinIO) with AES-256-GCM |
| Verification | every upload's server checksum compared with the client's; every download SHA-256-verified |

### Throughput, consistency and stability

| Run | Storage machines | Upload MB/s | Download MB/s (30 s) | Files moved | Errors | Checksum mismatches | Raft term | Ring epoch | Worst stall |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 60 | 1129 | 1227 | 2431 | 0 | 0 | 2 → 2 | 60 → 60 | 31 ms |
| 2 | 60 | 1141 | 1231 | 2430 | 0 | 0 | 2 → 2 | 60 → 60 | 7 ms |
| 3 | 60 | 1100 | 1225 | 2425 | 0 | 0 | 2 → 2 | 60 → 60 | 18 ms |

Median upload **1,129 MB/s**, median download **1,227 MB/s**, with 7,286
files moved and every one verified.

**Repair** (third run, one storage machine SIGKILLed): the victim held 113
chunks (452 MiB). It was marked DOWN after 4.83 s (`dead-after` is 5 s). A
0.94 s repair pass re-replicated all 113 chunks, so replication was restored
6.25 s after the kill. A verification pass found 0 of 2,304 chunks
under-replicated, and all 40 sampled files read back correctly.

### What limits these numbers

- **The single client machine's network link.** ~1.2 GB/s is ~10 Gb/s, the
  same in every run, with the cluster's per-node load tiny (each storage node
  served ~20 MB/s). One client cannot show the cluster's ceiling. Several
  client machines would be the next experiment.
- **Upload duration.** 3 GiB went up in under 3 s, so the upload figure is a
  short burst. The 30 s download phase is the sustained number.
- **S3 write-behind lags.** All chunks reached the S3 tier about 60 s after
  the uploads finished, because one MinIO process on one machine takes
  every copy (~50 MB/s). Replicas are written first, so this doesn't slow
  uploads. It is the window in which an all-replicas loss could not yet be
  restored from S3.

### The control plane was quiet on dedicated machines

The Raft term stayed at 2 (the initial election) and the ring epoch at 60
(one join per node) through all three runs. So there were no leader
elections and no node falsely marked DOWN, and the worst scheduling stall
was 31 ms. Compare the single-laptop runs below, where stalls reached
hundreds of ms and caused elections. The timing profile was right; the
laptop was oversubscribed.

## Raft leader recovery

On one Linux host (20 cores, 61 GB RAM): 3 `dfs-meta` processes with durable
Raft storage (fsync on) and 10 storage nodes heartbeating to the leader.
Each trial removes the leader, then measures the time until a client's first
successful Raft-committed write, including finding the new leader. 30
trials per configuration.

| Configuration | Failure | p50 | p90 | max | mean |
|---|---|---|---|---|---|
| hashicorp defaults (1 s HB), BoltDB | SIGKILL | 2089 ms | 2687 ms | 2838 ms | 2089 ms |
| fast timing, BoltDB | SIGKILL | 416 ms | 539 ms | 589 ms | 420 ms |
| fast timing, WAL + single-fsync stable store | SIGKILL | 413 ms | 527 ms | 540 ms | 408 ms |
| fast timing, WAL, crash detection (**default**) | SIGKILL | **193 ms** | 228 ms | 1789 ms | 242 ms |
| default config | SIGSTOP (hung, sockets open) | 412 ms | 615 ms | 724 ms | 433 ms |
| default config | SIGTERM (planned) | 5 ms | 6 ms | 7 ms | 5 ms |
| aggressive timing (100 ms HB), WAL, crash detection | SIGKILL | 92 ms | 102 ms | 105 ms | 91 ms |

- **Baseline → default.** Stock settings take about 2.1 s to restore writes
  after a leader crash. LAN timing brings that to about 0.4 s. Crash
  detection from connection loss halves it again, to a 193 ms median,
  because followers learn the leader died from the kernel's FIN instead of
  waiting out a randomized 200–400 ms heartbeat timeout.
- **The one slow trial** (1.8 s of 30) is an election that failed and waited
  out the 1 s election timeout before retrying. That's the cost of a long
  election timeout; the benefit is that it stopped the election storms under
  load (below).
- **The WAL store** barely matters on Linux, where `fsync` is cheap. On macOS
  each `F_FULLFSYNC` costs 5–8 ms and BoltDB needs two per write, so there
  the same change moved p50 from 307 to 224 ms with the 100 ms profile.
- **Freeze** (leader hung, sockets open) gets no connection-loss signal and
  falls back to the heartbeat timeout: a ~400 ms median.
- **Graceful** (SIGTERM) hands leadership over before exiting: 5 ms, which
  is what a rolling restart in Kubernetes would see.
- **Aggressive** (100 ms heartbeat) is faster on a quiet host (92 ms). It is
  not the default because it had spurious elections on the overloaded
  laptop, where stalls reached hundreds of ms.

## Kubernetes (kind, 4 nodes on one Linux host)

`scripts/kind-e2e.sh` builds the image and creates a fresh kind cluster (1
control plane + 3 workers). It applies `deploy/k8s` (Cassandra, MinIO, the
Raft StatefulSet and the storage StatefulSet), then exercises it:

| Step | Result |
|---|---|
| Deploy | all StatefulSets rolled out; Raft elected a leader over the pods' stable DNS names |
| Smoke Job, 6 storage pods | 40 files × 8 MiB written and read back by 8 clients, all matching |
| `kubectl scale` storage to 50 | 50 pods joined the ring, spread 17 / 16 / 17 across the three workers |
| Throughput Job on 50 storage pods | 408 MB/s upload, **1,472 MB/s download** (30 s), 0 errors, 0 checksum mismatches |
| Storage pod force-deleted during a smoke Job | the Job still passed; the pod came back with the same identity and volume in 5.8 s, inside `dead-after`, so the ring never changed |
| Raft leader pod force-deleted | new leader seen 0.17 s later (term 2 → 3) |

All of it runs on one host, so the throughput reflects that host's CPU and
disk, not a network. For the leader-pod deletion, Kubernetes delivers SIGTERM
before killing the container, and `dfs-meta` hands leadership over on
SIGTERM. So this likely measured the planned-handover path. The crash
(SIGKILL) numbers are in the failover table above.

## Earlier: 50–64 processes on one laptop

Before the Slurm deployment, the same benchmarks ran as 53–67 OS processes
on a 16 GB Apple M4 laptop, sharing its CPU and SSD with Cassandra, MinIO
and the user's other containers. That surfaced most of the bugs fixed during
development (table below). But the throughput there was capped by the SSD's
writeback, and varied 70–500 MB/s from run to run. `close()` on a freshly
written 4 MiB file had a p99 of 1–2 s with 50 concurrent writers, and the
receiving node spent most of a slow upload in exactly that step. Those
numbers are not reported as results.

| Finding | Fix |
|---|---|
| Uploads stalled 13 s: gocql dialled hosts it discovered on its default port 9042, not the configured one | Set `ClusterConfig.Port` from the contact points |
| Committed files unreadable after membership churn: chunks sat outside the 5 nodes the new ring pointed at | Reads locate holders on every known node before trying S3 (`TestReadsDuringRingChurnBeforeRepair`) |
| Storage nodes burned 7–8 cores to move 20 MB/s: 53 processes each with `GOMAXPROCS=10` spun in the Go scheduler | `GOMAXPROCS=2` per process (what Go picks for a 2-CPU pod); upload 20 → 91 MB/s in the same run |
| Election storms (term 2 → 23 in one run): a 100 ms election timeout was shorter than a vote's fsync under load | 1 s election timeout (free on success), 200 ms heartbeat |
| A third of failovers took the slow path: an old restore timer reset the crash-detect window mid-failover | Generation counter on the window |
| Duplicate S3 uploads after elections | Write-behind skips chunks a repair pass already uploaded |
| On Slurm, fixed ports collided with node_exporter (9100), and some nodes advertised an address other nodes couldn't reach | Each process takes a free port; addresses come from the network all nodes share |

## DHT routing (unit tests)

From `internal/ring` with 128 virtual nodes:

- 50 nodes, 200k chunks at RF=3: per-node load coefficient of variation 5.6%.
- Removing 1 of 50 nodes changed 5.65% of replica sets (3/50 = 6% expected),
  and no replica set that excluded the removed node.
- `Lookup` takes 0.4 µs for a 3-replica preference list on a 50-node ring.

## Reproduce

On a Slurm cluster (from the login node, after copying `bin/` and
`deploy/slurm/` to `$DFS_HOME` and pulling `cassandra.sif` and `minio.sif`
into `$DFS_HOME/sif`):

```bash
ACCOUNT=<your-account> deploy/slurm/launch.sh 60
```

```bash
deploy/slurm/bench.sh 60 3
```

Raft failover matrix on one host:

```bash
ONLY=failover-baseline,failover-fast-bolt,failover-fast-wal,failover-final,failover-freeze,failover-graceful,failover-aggressive scripts/run-benchmarks.sh
```

```bash
python3 scripts/results-table.py
```
