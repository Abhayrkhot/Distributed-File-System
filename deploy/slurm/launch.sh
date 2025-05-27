#!/usr/bin/env bash
# Launch a DFS cluster across many machines of a Slurm cluster, one role per
# machine. Run on the login node.
#
#   deploy/slurm/launch.sh 60          # 60 storage machines + 3 Raft + 1 backends
#
# Partitions such as Explorer's `short` cap jobs at 2 nodes, so the cluster
# is many 2-node jobs. They are submitted one at a time, each excluding the
# nodes earlier jobs got, so no two roles share a machine. Storage nodes join
# the ring whenever their job starts.
#
# Env: ACCOUNT, PARTITION (short), TIME (02:00:00), CPUS (4), MEM (8G),
#      DFS_HOME (/scratch/$USER/dfs)
set -euo pipefail
N=${1:-60}
ACCOUNT=${ACCOUNT:?set ACCOUNT to your Slurm account}
PARTITION=${PARTITION:-short}
TIME=${TIME:-02:00:00}
DFS_HOME=${DFS_HOME:-/scratch/$USER/dfs}
RUN=$DFS_HOME/runs/$(date +%Y%m%d-%H%M%S)
mkdir -p "$RUN"/{addr,kill,restart,logs}
echo "$RUN" >"$DFS_HOME/runs/latest"
used=""

submit() { # cpus mem roles...
  local cpus=$1 mem=$2; shift 2
  local n=$#
  local jid
  jid=$(sbatch --parsable -J "dfs-${1#*:}" -p "$PARTITION" -A "$ACCOUNT" -t "$TIME" \
    -N "$n" --ntasks-per-node=1 -c "$cpus" --mem="$mem" ${used:+--exclude=$used} \
    -o "$RUN/logs/slurm-%j.out" --export=ALL,RUN="$RUN",DFS_HOME="$DFS_HOME",ROLES="$*" \
    "$DFS_HOME/deploy/slurm/job.sbatch")
  local state=""
  until [ "$state" = RUNNING ]; do
    state=$(squeue -h -j "$jid" -o %T)
    [ -z "$state" ] && { echo "job $jid ended before starting; see $RUN/logs/slurm-$jid.out" >&2; exit 1; }
    sleep 2
  done
  local nl
  nl=$(squeue -h -j "$jid" -o %N)
  used=${used:+$used,}$(scontrol show hostnames "$nl" | paste -sd,)
  echo "$jid $nl $*" | tee -a "$RUN/jobs"
}

echo "run dir: $RUN"
submit "${BACKEND_CPUS:-16}" "${BACKEND_MEM:-32G}" backends:backends meta:meta-0
submit "${CPUS:-4}" "${MEM:-8G}" meta:meta-1 meta:meta-2
for ((i = 0; i < N; i += 2)); do
  a=$(printf 'node-%02d' "$i")
  if ((i + 1 < N)); then
    submit "${CPUS:-4}" "${MEM:-8G}" "storage:$a" "storage:$(printf 'node-%02d' $((i + 1)))"
  else
    submit "${CPUS:-4}" "${MEM:-8G}" "storage:$a"
  fi
done
echo "submitted; $(wc -l <"$RUN/jobs") jobs on $(echo "$used" | tr ',' '\n' | sort -u | wc -l) distinct machines"
echo "stop with: touch $RUN/stop  (then scancel any stragglers: scancel -n dfs-...)"
