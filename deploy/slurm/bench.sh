#!/usr/bin/env bash
# Run the throughput benchmark against a cluster started by launch.sh.
# The client runs as a step in the control job, on the meta-0 machine (the
# backends machine is busy with Cassandra and MinIO).
#
#   deploy/slurm/bench.sh [storage-nodes] [runs]
set -euo pipefail
N=${1:-60}
RUNS=${2:-3}
DFS_HOME=${DFS_HOME:-/scratch/$USER/dfs}
RUN=${RUN:-$(cat "$DFS_HOME/runs/latest")}
OUT=${OUT:-$DFS_HOME/results}
mkdir -p "$OUT"
read -r jid nl _ <"$RUN/jobs"
client=$(scontrol show hostnames "$nl" | sed -n 2p) # meta-0's machine
meta=$("$DFS_HOME/deploy/slurm/meta-addrs.sh" "$RUN")
machines=$(cut -d' ' -f2 "$RUN/jobs" | xargs -n1 scontrol show hostnames | sort -u | wc -l)
desc="Explorer HPC (Northeastern), Slurm: $N storage machines + 3 Raft machines + 1 backends machine ($machines distinct), one role per machine, client on the meta-0 machine"
echo "run=$RUN client=$client meta=$meta machines=$machines"
srun --jobid="$jid" --overlap -N1 -n1 -w "$client" bash -c "lscpu | grep -E 'Model name|^CPU\(s\)'; free -g | sed -n 2p" | tee "$OUT/client-node.txt"

for r in $(seq 1 "$RUNS"); do
  repair=false
  [ "$r" = "$RUNS" ] && repair=true
  srun --jobid="$jid" --overlap -N1 -n1 -w "$client" -c 16 bash -c "
    unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
    $DFS_HOME/bin/dfs-bench throughput --cluster $meta --storage $N --wait 20m \
      --clients 64 --file-size 16MiB --total 3072MiB --download-for 30s --repair=$repair \
      --kill-cmd '$DFS_HOME/deploy/slurm/kill-node.sh $RUN {id}' \
      --host-desc '$desc' --out $OUT --label hpc-${N}machines-r$r" 2>&1 |
    tee "$OUT/hpc-${N}machines-r$r.log" | grep -vE ' bench/'
done
