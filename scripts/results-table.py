#!/usr/bin/env python3
"""Print the markdown tables in docs/results.md from bench/results/*.json."""
import glob
import json
import os
import sys

root = sys.argv[1] if len(sys.argv) > 1 else "bench/results"


def load(pattern):
    out = []
    for p in sorted(glob.glob(os.path.join(root, pattern))):
        with open(p) as f:
            out.append((os.path.basename(p), json.load(f)))
    return out


print("### Throughput\n")
print("| Storage nodes | Upload MB/s | Download MB/s | Upload p50 / p99 per 16 MiB file | Errors | Checksum mismatches | Raft term change | S3 drain after upload |")
print("|---|---|---|---|---|---|---|---|")
for name, d in load("throughput-*.json"):
    u, dl = d["upload"], d["download"]
    print(f"| {d['storage_nodes']} | {u['mb_per_s']:.0f} | {dl['mb_per_s']:.0f} | "
          f"{u['per_file_latency_ms']['p50']/1000:.1f} s / {u['per_file_latency_ms']['p99']/1000:.1f} s | "
          f"{u['errors'] + dl['errors']} | {dl['checksum_mismatches']} | "
          f"{d['raft_term_start']} → {d['raft_term_end']} | {d['s3_writebehind_drain_ms']/1000:.1f} s |")

print("\n### Repair after a storage-node SIGKILL\n")
print("| Storage nodes | Victim held | Marked DOWN after | Repair pass | Replication restored after kill | Verify pass under-replicated | Reads after repair |")
print("|---|---|---|---|---|---|---|")
for name, d in load("throughput-*.json"):
    r = d.get("repair")
    if not r:
        continue
    print(f"| {d['storage_nodes']} | {r['victim_chunks']} chunks ({r['victim_bytes'] >> 20} MiB) | {r['detect_ms']/1000:.2f} s | "
          f"{r['repair_pass_ms']:.0f} ms | {r['restored_ms']/1000:.2f} s | {r['verify_under_replicated']} of {r['verify_chunks_scanned']} | "
          f"{r['files_read_back_after_repair'] - r['read_errors_after_repair']}/{r['files_read_back_after_repair']} OK |")

print("\n### Raft leader recovery (30 trials each)\n")
print("| Configuration | Failure | p50 | p90 | max | mean |")
print("|---|---|---|---|---|---|")
order = ["baseline", "fast-bolt", "fast-wal", "final", "freeze", "graceful", "aggressive"]
rows = {}
for name, d in load("failover-*.json"):
    for label in order:
        if name.endswith(f"-{label}.json"):
            rows[label] = d
desc = {
    "baseline": "hashicorp defaults (1 s HB), BoltDB",
    "fast-bolt": "fast timing, BoltDB",
    "fast-wal": "fast timing, WAL + single-fsync stable store",
    "final": "fast timing, WAL, crash detection (**default**)",
    "freeze": "default config",
    "graceful": "default config",
    "aggressive": "aggressive timing (100 ms HB), WAL, crash detection",
}
fail = {"crash": "SIGKILL", "freeze": "SIGSTOP (hung, sockets open)", "term": "SIGTERM (planned)"}
for label in order:
    d = rows.get(label)
    if not d:
        continue
    r = d["recovery_ms"]
    print(f"| {desc[label]} | {fail[d['mode']]} | {r['p50']:.0f} ms | {r['p90']:.0f} ms | {r['max']:.0f} ms | {r['mean']:.0f} ms |")
