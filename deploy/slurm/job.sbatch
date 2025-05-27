#!/usr/bin/env bash
# One Slurm job = N nodes, one role per node. Submitted by launch.sh with
# ROLES="role:name role:name ..." (in node order), RUN and DFS_HOME exported.
#SBATCH --ntasks-per-node=1
set -uo pipefail
mapfile -t nodes < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
i=0
for rn in $ROLES; do
  srun --nodes=1 --ntasks=1 --nodelist="${nodes[$i]}" --cpus-per-task="$SLURM_CPUS_PER_TASK" \
    "$DFS_HOME/deploy/slurm/node-run.sh" "${rn%%:*}" "${rn#*:}" &
  i=$((i + 1))
done
wait
