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


import statistics


def med(xs):
    return statistics.median(xs)


# ---- Many machines (Slurm) ----
hpc_root = os.path.join(os.path.dirname(root.rstrip("/")) or ".", "results-hpc")
hpc = []
for p in sorted(glob.glob(os.path.join(hpc_root, "throughput-hpc-*.json"))):
    with open(p) as f:
        hpc.append(json.load(f))
if hpc:
    print("### 60 machines (Explorer HPC, Slurm)\n")
    print("| Run | Storage machines | Upload MB/s | Download MB/s (30 s) | Files moved | Errors | Checksum mismatches | Raft term | Ring epoch | Worst stall |")
    print("|---|---|---|---|---|---|---|---|---|---|")
    for i, d in enumerate(hpc, 1):
        u, dl = d["upload"], d["download"]
        stall = max((c.get("max_timer_late_ms", 0) for c in d.get("cpu_samples", [])), default=0)
        print(f"| {i} | {d['storage_nodes']} | {u['mb_per_s']:.0f} | {dl['mb_per_s']:.0f} | {u['files'] + dl['files']} | "
              f"{u['errors'] + dl['errors']} | {dl['checksum_mismatches']} | {d['raft_term_start']} → {d['raft_term_end']} | "
              f"{d['ring_epoch_start']} → {d['ring_epoch_end']} | {stall:.0f} ms |")
    ups = [d["upload"]["mb_per_s"] for d in hpc]
    dns = [d["download"]["mb_per_s"] for d in hpc]
    print(f"\nMedian upload {statistics.median(ups):.0f} MB/s, median download {statistics.median(dns):.0f} MB/s.\n")
    for d in hpc:
        r = d.get("repair")
        if r:
            print("**Repair (run with a SIGKILLed storage node):** "
                  f"victim held {r['victim_chunks']} chunks ({r['victim_bytes'] >> 20} MiB); marked DOWN after {r['detect_ms']/1000:.2f} s; "
                  f"repair pass {r['repair_pass_ms']:.0f} ms re-replicated {r['replicas_repaired']}; replication restored "
                  f"{r['restored_ms']/1000:.2f} s after the kill; verify pass: {r['verify_under_replicated']} of {r['verify_chunks_scanned']} under-replicated; "
                  f"{r['files_read_back_after_repair'] - r['read_errors_after_repair']}/{r['files_read_back_after_repair']} files read back OK.\n")

tp = load("throughput-*nodes-r*.json")
by_n = {}
for name, d in tp:
    by_n.setdefault(d["storage_nodes"], []).append(d)

if tp:
    print("### Single host (median of runs, [min–max])\n")
if tp:
    print("| Storage nodes | Runs | Upload MB/s | Download MB/s | Upload per-file p50 | Transfer errors | Checksum mismatches | S3 drain after upload |")
    print("|---|---|---|---|---|---|---|---|")
for n in sorted(by_n):
    ds = by_n[n]
    up = [d["upload"]["mb_per_s"] for d in ds]
    dn = [d["download"]["mb_per_s"] for d in ds]
    p50 = [d["upload"]["per_file_latency_ms"]["p50"] / 1000 for d in ds]
    errs = sum(d["upload"]["errors"] + d["download"]["errors"] for d in ds)
    mism = sum(d["download"]["checksum_mismatches"] for d in ds)
    drain = [d["s3_writebehind_drain_ms"] / 1000 for d in ds]
    files = sum(d["upload"]["files"] + d["download"]["files"] for d in ds)
    print(f"| {n} | {len(ds)} | {med(up):.0f} [{min(up):.0f}–{max(up):.0f}] | {med(dn):.0f} [{min(dn):.0f}–{max(dn):.0f}] | "
          f"{med(p50):.1f} s | {errs} of {files} files | {mism} | {med(drain):.1f} s |")

if tp:
    print("\n### Single-host repair after a storage-node SIGKILL (median of runs)\n")
    print("| Storage nodes | Victim held | Marked DOWN after | Repair pass | Replication restored after kill | Verify pass under-replicated | Reads after repair |")
    print("|---|---|---|---|---|---|---|")
for n in sorted(by_n):
    rs = [d["repair"] for d in by_n[n] if d.get("repair")]
    if not rs:
        continue
    print(f"| {n} | {med([r['victim_chunks'] for r in rs]):.0f} chunks | {med([r['detect_ms'] for r in rs])/1000:.2f} s | "
          f"{med([r['repair_pass_ms'] for r in rs]):.0f} ms | {med([r['restored_ms'] for r in rs])/1000:.2f} s | "
          f"{max(r['verify_under_replicated'] for r in rs)} (all runs) | "
          f"{sum(r['files_read_back_after_repair'] - r['read_errors_after_repair'] for r in rs)}/{sum(r['files_read_back_after_repair'] for r in rs)} OK |")

if tp:
    print("\n### Single-host control-plane stability under load\n")
    print("| Storage nodes | Run | Raft term start → end | Ring epoch start → end | Worst scheduling stall |")
    print("|---|---|---|---|---|")
for n in sorted(by_n):
    for i, d in enumerate(by_n[n], 1):
        stall = max((c.get("max_timer_late_ms", 0) for c in d.get("cpu_samples", [])), default=0)
        print(f"| {n} | {i} | {d['raft_term_start']} → {d['raft_term_end']} | {d['ring_epoch_start']} → {d['ring_epoch_end']} | {stall:.0f} ms |")

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
