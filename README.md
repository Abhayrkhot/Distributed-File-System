# Distributed File System

A replicated, fault-tolerant file service in Go. Files are split into
content-addressed chunks and routed over a consistent-hash DHT to storage
nodes. Each chunk is replicated three ways with quorum writes and backed by
an encrypted S3 tier. A Raft control plane tracks membership and repairs
replication after failures, and Cassandra holds the file metadata. Everything
talks gRPC, and it deploys to Kubernetes.

**Go · gRPC · Raft (hashicorp/raft) · Cassandra · S3 (MinIO/AWS) · Kubernetes**

## Results

Measured on **64 separate machines** of a Slurm HPC cluster (60 storage
nodes, 3 Raft members, 1 Cassandra + MinIO), and on one Linux host for Raft
failover. Details and caveats are in [docs/results.md](docs/results.md).

| | |
|---|---|
| Aggregate throughput, 60 storage machines | **1,129 MB/s upload, 1,227 MB/s download** (median of 3 runs); every byte replicated 3× plus an S3 copy, and every download SHA-256-verified |
| Correctness under load | 7,286 files moved, 0 errors, 0 checksum mismatches |
| Storage-node crash | marked DOWN in 4.8 s; 113 chunks re-replicated in a 0.94 s pass; 0 of 2,304 chunks under-replicated afterwards |
| Raft leader crash → writes again | **193 ms** median (p90 228 ms) over 30 SIGKILLs, against 2.1 s with hashicorp/raft defaults; 5 ms for a planned handover |
| Control-plane stability | 0 leader elections and 0 false node evictions during the 60-machine runs |
| AWS S3 tier | integration suite passes against AWS S3, including restoring chunks from S3 after all three replicas were lost |
| Kubernetes (kind, 4 nodes) | scaled to 50 storage pods; 1,472 MB/s download with 0 errors; a storage pod and the Raft leader pod force-deleted with no client errors |

The aggregate rate is bounded by the single client machine's ~10 Gb/s
link, not by the cluster.

## How it works

```
 client ──gRPC──▶ any storage node (coordinates)
                    │  chunk = sha256(4 MiB)  →  ring.Lookup(id, 3)
                    ├──▶ replica 1 ┐
                    ├──▶ replica 2 ├─ ack after W=2 of N=3
                    └──▶ replica 3 ┘      └─ primary: write-behind to S3 (AES-256-GCM)
                    └──▶ Cassandra: commit manifest (only after every chunk has quorum)

 storage nodes ──heartbeats──▶ Raft leader ──commits membership (ring epoch)──▶ all nodes
                               └── repair: restore 3 replicas from peers, or from S3
```

- **DHT routing.** A consistent-hash ring with 128 virtual nodes per member.
  The Raft group publishes membership and every node routes locally (a
  one-hop DHT). Load varies 5.6% across 50 nodes, and removing a node moves
  only the ~6% of replica sets that included it.
- **Replication and consistency.** Chunks are immutable and named by their
  SHA-256, so replicas never diverge. Writes return after a W=2 of N=3 quorum
  and fall back to a sloppy quorum when preference nodes are down. A file's
  manifest is committed last, as one Cassandra row, so readers see the old
  version or the new one, never a torn file. Every chunk is hash-checked on
  read, and the read moves on to the next replica on a mismatch.
- **Raft control plane.** Three members. The leader turns heartbeats into
  UP/DOWN membership changes, redirects clients, and runs repair. The data
  plane routes by a cached ring, so uploads and downloads continue through
  leader elections and even with the whole control plane down.
- **Faster leader recovery.** Timing tuned for a LAN, a Raft log and term
  store that sync once per write instead of twice, and crash detection from
  the leader's TCP connection closing (see
  [docs/design.md](docs/design.md#faster-leader-recovery)).
- **S3 tier.** Write-behind copies of every chunk, client-side AES-256-GCM
  with the object key bound as AEAD data. When all three replicas of a chunk
  are lost, repair restores it from S3.

The full design, consistency model and failure matrix are in
[docs/design.md](docs/design.md). Methodology and raw numbers are in
[docs/results.md](docs/results.md).

## Try it

Requires Go 1.26+.

```bash
make build
```

```bash
bin/dfs-local --storage 6
```

`dfs-local` runs 3 Raft members and 6 storage nodes in one process, with an
in-memory metastore and a directory-backed S3 tier. It prints a
`DFS_META=...` line; export it, then in another shell:

```bash
bin/dfs status
```

```bash
bin/dfs put ./some.file demo/some.file
```

```bash
bin/dfs get demo/some.file ./copy.file
```

Other commands: `dfs ls [prefix]`, `dfs stat <name>` (shows each chunk's
replica nodes), `dfs rm <name>`, `dfs repair`, `dfs transfer-leader`.

### With Cassandra and MinIO

`scripts/dev-deps.sh up` runs Homebrew Cassandra (port 19042) and MinIO
(port 19000) from `.local/` and prints the environment the binaries read.
Then start the real processes:

```bash
eval "$(scripts/dev-deps.sh env)"
P=meta-0=127.0.0.1:7100=127.0.0.1:8100,meta-1=127.0.0.1:7101=127.0.0.1:8101,meta-2=127.0.0.1:7102=127.0.0.1:8102
for i in 0 1 2; do bin/dfs-meta --id meta-$i --raft-bind 127.0.0.1:710$i --grpc-bind 127.0.0.1:810$i \
  --peers $P --bootstrap --data-dir /tmp/dfs/meta-$i & done
for i in 0 1 2 3 4 5; do bin/dfs-storage --id node-0$i --bind 127.0.0.1:910$i --data-dir /tmp/dfs/node-0$i \
  --meta 127.0.0.1:8100,127.0.0.1:8101,127.0.0.1:8102 & done
```

## Tests

```bash
make test
```

```bash
make integration
```

`make test` runs unit tests and the integration suite on an in-process
cluster: real gRPC and Raft over loopback, no external services.
`make integration` runs the same suite against real Cassandra and MinIO. The
integration tests cover:

| Area | Tests |
|---|---|
| Transfers | round trips at chunk boundaries, 24 concurrent clients, versions, delete, dedup |
| Replication | every chunk on exactly its 3 preference nodes; reads survive losing any 2 nodes; writes continue with 2 nodes down (sloppy quorum) |
| Consistency | no quorum → no visible file; readers racing 24 concurrent overwrites never see a torn file; a corrupt replica is skipped |
| Recovery | dead node detected and re-replicated; node rejoin back-fill; all 3 replicas lost → restored from S3; ring growth before repair |
| Raft | leader crash during live traffic (0 data-plane errors); planned leadership transfer; repair under a new leader; data plane with the control plane down |

## Benchmarks

Across many machines on a Slurm cluster, one role per machine
(`deploy/slurm/`; partitions that cap jobs at 2 nodes are handled by
submitting many 2-node jobs):

```bash
ACCOUNT=<your-account> deploy/slurm/launch.sh 60
```

```bash
deploy/slurm/bench.sh 60 3
```

On one host (Raft failover matrix, plus a process-per-node throughput run):

```bash
scripts/run-benchmarks.sh
```

The JSON results are in `bench/results-hpc/` and `bench/results/`, and
`scripts/results-table.py` renders the tables in
[docs/results.md](docs/results.md).

## Kubernetes

`deploy/k8s` is a kustomization: a 3-member Raft StatefulSet (stable DNS
names, PVCs, PodDisruptionBudget), a storage StatefulSet (scale it with
`kubectl scale`), Cassandra and MinIO, plus a smoke-test Job.

`scripts/kind-e2e.sh` does the whole thing on a fresh 4-node kind cluster:
deploy, smoke test, scale storage to 50 pods, a throughput Job, then
force-delete a storage pod mid-traffic and the Raft leader pod (results in
[docs/results.md](docs/results.md#kubernetes-kind-4-nodes-on-one-linux-host)).

```bash
scripts/kind-e2e.sh
```

Or step by step:

```bash
make kind-up
```

```bash
make kind-deploy
```

```bash
make kind-smoke
```

On Linux, multi-node kind needs a higher inotify limit
(`fs.inotify.max_user_instances=512`), or kube-proxy on the workers fails
with "too many open files" and pods can't resolve DNS.

## Layout

```
cmd/dfs-meta       Raft control-plane member
cmd/dfs-storage    storage node (ChunkService + FileService)
cmd/dfs            CLI
cmd/dfs-local      whole cluster in one process
cmd/dfs-bench      throughput / failover / k8s smoke benchmarks
internal/ring      consistent-hash DHT
internal/cas       content-addressed chunk store
internal/coord     quorum writes, verified reads
internal/storage   storage node
internal/meta      Raft FSM, liveness, leader redirects, stable store, transport
internal/repair    replica repair
internal/metastore Cassandra + in-memory metadata
internal/blob      S3 / directory tier, AES-256-GCM
internal/cluster   in-process cluster for tests
proto/dfs/v1       gRPC API
deploy/k8s         Kubernetes manifests
```

The first commit is the original single-process prototype: a TCP
peer-to-peer store adapted from a tutorial. Its content-addressed path
scheme and encryption survive, reworked, in `internal/cas` and
`internal/blob`.
