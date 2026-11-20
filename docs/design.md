# Design

A distributed file service in Go. Files are split into content-addressed
chunks, replicated across storage nodes chosen by a consistent-hash ring, and
backed by an S3 tier. A small Raft group owns cluster membership and repairs
replication after failures. Metadata (which chunks make up a file) lives in
Cassandra.

```
              clients (dfs CLI, client library)
                         │ gRPC FileService (any node)
        ┌────────────────┼────────────────────────────┐
        ▼                ▼                            ▼
  ┌──────────┐     ┌──────────┐                 ┌──────────┐
  │ storage  │◀───▶│ storage  │◀──── ... ──────▶│ storage  │   50+ nodes
  │ node-00  │     │ node-01  │  ChunkService   │ node-49  │   local CAS on disk
  └────┬─────┘     └────┬─────┘  (gRPC streams) └────┬─────┘
       │ heartbeats     │  ring watch                │ write-behind
       ▼                ▼                            ▼
  ┌─────────────────────────────┐            ┌──────────────┐
  │ Raft control plane (3)      │            │ S3 (MinIO)   │
  │ membership · ring epochs ·  │──repair───▶│ AES-256-GCM  │
  │ liveness · repair (leader)  │            │ chunk objects│
  └─────────────────────────────┘            └──────────────┘
                         │ manifests, chunk catalogue
                         ▼
                   ┌────────────┐
                   │ Cassandra  │
                   └────────────┘
```

## Components

| Package | Role |
|---|---|
| `internal/ring` | Consistent-hash ring, 128 virtual nodes per member. `Lookup(key, n)` returns the first n distinct members clockwise from the key: the chunk's preference list. |
| `internal/cas` | A node's local chunk store. Chunk ID = hex SHA-256 of the content; writes are verified against the ID and renamed into place, so a partial or corrupt chunk is never visible. Grew out of the prototype's CAS path transform. |
| `internal/storage` | Storage node: `ChunkService` (put/get/has/replicate/backup), `FileService` (coordinates client requests), heartbeat loop, ring watch, S3 write-behind workers. |
| `internal/coord` | Upload/download logic used by every node's FileService: chunking, quorum writes, verified reads with failover. |
| `internal/meta` | Raft control plane on hashicorp/raft: replicated membership FSM, leader-side liveness tracking, ring watch stream, leader redirects, repair scheduling. |
| `internal/repair` | Leader-run pass that restores N replicas per chunk, from peers or from S3. |
| `internal/metastore` | File manifests and chunk catalogue: Cassandra, plus an in-memory implementation that passes the same conformance suite. |
| `internal/blob` | S3 tier: S3/MinIO, a directory-backed store for tests, and an AES-256-GCM wrapper. |

## DHT routing

Routing is a one-hop DHT in the Dynamo/Cassandra style. The control plane
publishes the membership; each participant builds the same ring from it and
routes a chunk with a local lookup. Nothing on the data path asks a directory
where a chunk lives.

- 128 virtual nodes per member keeps load even: across 50 nodes and 200k
  chunks at RF=3, the coefficient of variation of per-node load is 5.6%
  (`TestBalance`).
- A membership change only moves the keys whose preference list included the
  changed node: removing 1 of 50 nodes changed 5.65% of replica sets, against
  an expected 3/50 = 6% (`TestMinimalMovement`).
- Ring members are the UP nodes. A node the leader marks DOWN drops out at
  the next epoch, and its ranges pass to the next nodes clockwise.

## Write path

1. A client streams the file to any storage node, which coordinates.
2. The coordinator cuts the stream into 4 MiB chunks; the chunk ID is
   `sha256(chunk)`. Up to 4 chunks per upload are in flight.
3. Each chunk goes to its N=3 preference-list nodes in parallel (gRPC
   streams of 1 MiB frames; a replica on the coordinator itself is written
   locally). The write returns once **W=2** replicas have acknowledged. The
   third keeps writing in the background.
4. If fewer than W preference replicas succeed, the coordinator continues
   clockwise past the preference list (a sloppy quorum). The next repair
   pass moves those copies back where the ring wants them.
5. W is never lowered when the ring shrinks. Acknowledging a write with
   fewer copies than W would silently reduce durability, so a ring smaller
   than W fails writes (`TestNoQuorumNoPartialFile`).
6. Once every chunk has reached quorum, the coordinator records the chunks in
   the catalogue and commits the manifest: one Cassandra row holding the
   ordered chunk list, size and whole-file SHA-256, under a new `timeuuid`
   version.
7. The preference list's first replica also queues the chunk for the S3 tier
   (write-behind, AES-256-GCM, the object key bound as AEAD data).

## Read path

1. Fetch the manifest (latest version, or a named one).
2. Fetch chunks with 4-deep read-ahead and write them out in order. Each
   chunk is tried:
   - from the local store, if this node is in the preference list;
   - from the preference list in order, then two more ring nodes;
   - **from any known node that has it** (UP or DOWN), using one batched
     `HasChunks` call per node. This covers the window after membership churn
     and before repair has moved replicas onto their new preference lists.
     Without it, reads failed during the first 50-node benchmark
     (`TestReadsDuringRingChurnBeforeRepair` reproduces that failure);
   - finally, by asking a preference node to fetch it on the client's behalf,
     which can reach the S3 tier.
3. Every chunk's hash is checked on arrival, and the whole-file hash at the
   end. A corrupt replica fails its check, and the next replica is tried
   (`TestCorruptReplicaIsSkipped`).

## Consistency model

- **Chunks are immutable and content-addressed.** A replica holds either the
  exact bytes or nothing, so replicas never need to reconcile.
- **Manifest-last commit.** A manifest is written only after all its chunks
  are durable at quorum, and it is a single-partition insert. A reader sees
  the complete previous version or the complete new one, never a partial or
  mixed file (`TestConcurrentWritersSameName`: readers racing 24 concurrent
  overwrites of one name never saw a torn file).
- **Last writer wins per name.** Concurrent uploads of one name each create
  a version, and the highest `timeuuid` is "latest". Older versions stay
  readable by version ID.
- Cassandra is used at `QUORUM`. The local setup has one Cassandra node
  (RF=1), so there that means one replica.

## Control plane (Raft)

The replicated state is small: storage nodes (`id → addr, UP|DOWN`) and an
epoch counter. It is committed through hashicorp/raft on three members.

- **Liveness belongs to the leader.** Storage nodes heartbeat every 250 ms.
  The leader keeps last-seen times in memory (soft state). A node silent for
  `dead-after` (3 s in the benchmarks) is marked DOWN through Raft, and a
  returning node is marked UP. Each change bumps the ring epoch.
- **A new leader gives every node a full `dead-after` window** before marking
  anything DOWN. It has no heartbeat history, and without the grace window it
  would evict the whole cluster.
- **Followers redirect.** Leader-only RPCs on a follower fail with
  `FAILED_PRECONDITION` plus a `dfs-leader` trailer, and the client library
  follows it at once.
- **The data plane does not depend on Raft.** Nodes and coordinators route by
  their cached ring, so uploads and downloads continue through leader
  elections (`TestLeaderCrashFailover`: 0 failures in the transfers that ran
  during failover) and even with the whole control plane down
  (`TestDataPlaneSurvivesControlPlaneOutage`). Only membership changes and
  repair wait for a leader.

### Faster leader recovery

"Recovery" here means the time from the leader dying to the first
client-visible committed write on the new leader, including finding it. The
work behind it, each step measured in [results.md](results.md):

1. **Timing tuned for a LAN, chosen by measurement.** hashicorp/raft's
   defaults (1 s heartbeat timeout, 500 ms lease) are WAN-safe and recover in
   about 2 s. The `fast` profile uses a 200 ms heartbeat timeout, 100 ms
   leader lease and a **1 s election timeout**. The long election timeout is
   deliberate: a candidate wins the moment it has its votes, so the timeout
   only decides when a *failed* election retries. With a 100 ms election
   timeout, votes delayed by fsync under disk load made candidates give up on
   elections they would have won, and the cluster stormed (term 2 → 23 in one
   benchmark). The 200 ms heartbeat timeout came from a scheduling-stall probe
   on the benchmark host, which saw stalls up to ~250 ms under load. The
   `aggressive` profile (100 ms / 50 ms) recovers faster when idle but had
   spurious elections there. Pre-vote (on by default in hashicorp/raft 1.7+)
   stops a partitioned follower from disrupting the cluster.
2. **Fewer fsyncs on the election path.** A vote has to be durable before it
   is cast. raft-boltdb costs two fsyncs per write (data pages, then the meta
   page), and on macOS each `F_FULLFSYNC` measured 5–8 ms. An election writes
   term and vote about 6 times in sequence across candidate and voter, so the
   syncs alone added about 100 ms. The Raft log is now raft-wal (one sync per
   append), and term/vote go to `FileStableStore`. That is a two-slot,
   checksummed file that makes each write durable with exactly one fsync and
   survives a torn write (`TestFileStableStoreTornWrite`).
3. **Crash detection from connection loss.** A follower normally notices a
   dead leader only when its heartbeat timeout expires, which raft randomizes
   between 1× and 2× the timeout. But when the leader *process* crashes, the
   kernel closes its sockets at once, and the follower's read on the leader's
   replication connection returns EOF. The custom stream layer reports that
   EOF. For a short window (10 heartbeat timeouts, extended by every further
   loss) the follower then checks for leader contact with the lease-length
   timeout, 100 ms instead of 200 ms. It does this with `ReloadConfig`, which
   re-arms raft's follower timer immediately. A false alarm is harmless. A live leader keeps sending
   heartbeats every HeartbeatTimeout/10, so the check passes. And if one
   follower does call an election wrongly, pre-vote lets the other voters,
   which still hear the leader, refuse it. A hung leader whose sockets stay
   open (the `freeze` benchmark mode, SIGSTOP) gets no such signal, and
   recovery falls back to the normal heartbeat timeout.
4. **Planned handover.** SIGTERM on a leader transfers leadership before
   exiting, which skips detection entirely.

## Repair

The leader runs a repair pass 500 ms after every epoch change (debounced),
every 30 s, and on demand (`dfs repair`). A pass:

1. Lists chunk objects in S3, to know which chunks are backed up.
2. Pages through the chunk catalogue (Cassandra, 256 shards). For each page
   it computes every chunk's preference list on the current ring, then sends
   each node **one** batched `HasChunks` for the chunks it should hold.
3. Each node missing a chunk is told to pull it (`ReplicateChunk`) from a
   surviving replica, or from S3 if none survives. If a chunk has no holder
   on its preference list, every node is asked first, since a copy may sit
   outside it after churn or a sloppy write.
4. A holder is asked to upload any chunk with no S3 copy. That covers
   write-behind uploads that were dropped or still queued when their node
   died.
5. Work runs 16 tasks at a time, and every RPC has a deadline. A node that
   can't answer is skipped for that pass, not treated as missing data.

Tests cover a single node failure, a node rejoining, a leader crash during
repair, ring growth before repair, and the loss of all three replicas of a
chunk (restored from S3).

## Failure handling

| Failure | Effect | Handling |
|---|---|---|
| Storage node crash (undetected) | Writes to it fail fast | W=2 of 3 still met; sloppy quorum if 2 preference replicas are down |
| Storage node crash (after `dead-after`) | Leaves the ring | Repair re-replicates its chunks from peers |
| All replicas of a chunk lost | — | Repair restores from S3 |
| Corrupt replica on disk | Hash mismatch | Read moves to the next replica |
| Membership churn before repair | Chunks off their preference list | Reads locate holders on any known node |
| Raft leader crash | Membership frozen briefly | New leader elected (see results.md); data plane unaffected |
| Raft quorum lost | No membership changes or repair | Data plane keeps routing by its cached ring |
| Cassandra unavailable | Uploads can't commit manifests; reads can't find them | Fails the request. No partial file is visible |
| S3 unavailable | Write-behind fails | Repair re-requests backups later; replicas unaffected |

## Limits and non-goals

- **No chunk garbage collection.** Deleting or overwriting a file removes the
  manifest, not the chunks, which other files may share through dedup.
  Mark-and-sweep against live manifests would be the next step.
- **Extra copies are not trimmed.** After a node returns, or after sloppy
  writes, some chunks have more than three copies until GC exists.
- **No auth or TLS** on the gRPC services.
- **Listing scans.** `List` uses `SELECT DISTINCT` over partition keys, and
  repair scans the whole catalogue. Fine at this project's scale; a sharded
  name index and incremental repair would replace them.
- **One client in the benchmarks.** The 60-machine Slurm runs drive the
  cluster from a single client machine, whose ~10 Gb/s link caps the
  aggregate numbers (see [results.md](results.md)).
- **A few duplicate S3 uploads.** About 3% of chunks were uploaded to S3
  twice in the AWS run (197 uploads for 192 chunks). It is harmless, since
  objects are content-addressed, but wastes a little bandwidth.
- **One MinIO in the large runs.** The S3 tier in the 60-machine runs is a
  single MinIO process, so write-behind lags uploads by about a minute at
  1 GB/s. The tier was also verified against AWS S3 (see
  [results.md](results.md#aws-s3-as-the-s3-tier)), but at home-uplink speed.
