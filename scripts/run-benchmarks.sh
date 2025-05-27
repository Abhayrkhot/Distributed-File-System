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

run() { # name, command...
  local name=$1; shift
  echo "=== $name: $*"
  "$@" >"$OUT/$name.log" 2>&1 || { echo "FAILED: see $OUT/$name.log"; return 1; }
  grep -E '^(upload|download|repair|recovery|s3)' "$OUT/$name.log" | grep -v ' bench/' || true
}

# Throughput, S3 write-behind and repair on 50 and 64 storage processes.
run throughput-50nodes bin/dfs-bench throughput --storage 50 --clients 32 --total 1536MiB --download-for 30s --out "$OUT" --label 50nodes
run throughput-64nodes bin/dfs-bench throughput --storage 64 --clients 32 --total 1536MiB --download-for 30s --out "$OUT" --label 64nodes

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
