# Distributed File System

A replicated, fault-tolerant file service in Go. Files are split into
content-addressed chunks and routed over a consistent-hash DHT to storage
nodes. Each chunk is replicated three ways with quorum writes and backed by
an encrypted S3 tier. A Raft control plane tracks membership and repairs
replication after failures, and Cassandra holds the file metadata. Everything
talks gRPC, and it deploys to Kubernetes.

**Go · gRPC · Raft (hashicorp/raft) · Cassandra · S3 (MinIO/AWS) · Kubernetes**

<!-- RESULTS -->

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

```bash
scripts/run-benchmarks.sh
```

Reproduces every number in [docs/results.md](docs/results.md): throughput and
repair on 50 and 64 storage processes, and a Raft failover matrix with 30
trials per configuration. The JSON results are in `bench/results/`.

## Kubernetes

`deploy/k8s` is a kustomization: a 3-member Raft StatefulSet (stable DNS
names, PVCs, PodDisruptionBudget), a storage StatefulSet (scale it with
`kubectl scale`), Cassandra and MinIO, plus a smoke-test Job.

```bash
make kind-up
```

```bash
make kind-deploy
```

```bash
make kind-smoke
```

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
