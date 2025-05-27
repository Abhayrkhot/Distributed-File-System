#!/usr/bin/env bash
# Reproduce every number in docs/results.md. Takes ~25 minutes.
# Results land in bench/results/*.json; console output in bench/results/*.log.
set -euo pipefail
cd "$(dirname "$0")/.."
OUT=bench/results
mkdir -p "$OUT"
go build -o bin/ ./cmd/...
scripts/dev-deps.sh up >/dev/null
eval "$(scripts/dev-deps.sh env)"

# ONLY=name1,name2 reruns just those runs (names as in the "=== name" lines).
want() { [[ -z "${ONLY:-}" ]] || [[ ",$ONLY," == *",$1,"* ]]; }

run() { # name, command...
  local name=$1; shift
  want "$name" || return 0
  echo "=== $name: $*"
  "$@" >"$OUT/$name.log" 2>&1 || { echo "FAILED: see $OUT/$name.log"; return 1; }
  grep -E '^(upload|download|repair|recovery|s3)' "$OUT/$name.log" | grep -v ' bench/' || true
}

# Throughput, S3 write-behind and repair on 50 and 64 storage processes, three
# runs each: on one laptop SSD the results vary run to run with background
# writeback, so the docs report the median and range.
for n in 50 64; do
  for r in 1 2 3; do
    run throughput-${n}nodes-r$r bin/dfs-bench throughput --storage $n --clients 32 --total 1536MiB --download-for 30s --out "$OUT" --label ${n}nodes-r$r
  done
done

# Raft leader recovery: baseline, then each optimisation, then failure modes.
F="bin/dfs-bench failover --trials 30 --out $OUT"
run failover-baseline   $F --timing default --raft-store bolt --fast-crash-detect=false --label baseline
run failover-fast-bolt  $F --timing fast    --raft-store bolt --fast-crash-detect=false --label fast-bolt
run failover-fast-wal   $F --timing fast    --raft-store wal  --fast-crash-detect=false --label fast-wal
run failover-final      $F --timing fast    --raft-store wal  --label final
run failover-freeze     $F --timing fast    --raft-store wal  --mode freeze --label freeze
run failover-graceful   $F --timing fast    --raft-store wal  --mode term --label graceful
run failover-aggressive $F --timing aggressive --raft-store wal --label aggressive
echo "done"
