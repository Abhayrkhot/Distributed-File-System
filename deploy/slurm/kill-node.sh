#!/usr/bin/env bash
# kill-node.sh <run-dir> <name>: crash one role's process with SIGKILL, now.
# Runs a step inside the role's own Slurm job (srun --overlap), so it does
# not depend on shared-filesystem caching the way the $RUN/kill/<name> file
# does (NFS can hide a new file from other nodes for ~30 s).
set -euo pipefail
RUN=${1:?run dir} name=${2:?name}
line=$(grep -E "[: ]$name( |$)" "$RUN/jobs" | head -1)
[ -n "$line" ] || { echo "no job runs $name" >&2; exit 1; }
read -r jid nl roles <<<"$line"
idx=0
for r in $roles; do [ "${r#*:}" = "$name" ] && break; idx=$((idx + 1)); done
node=$(scontrol show hostnames "$nl" | sed -n "$((idx + 1))p")
# Called from inside another job's step: drop its SLURM_* so srun targets $jid.
for v in $(env | grep -o '^SLURM_[A-Z_]*'); do unset "$v"; done
srun --jobid="$jid" --overlap -N1 -n1 -w "$node" pkill -KILL -f -- "--id $name "
