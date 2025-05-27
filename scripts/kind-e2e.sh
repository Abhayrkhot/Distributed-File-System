#!/usr/bin/env bash
# End-to-end check on a local kind cluster (deploy/kind/cluster.yaml):
#   1. fresh cluster, build/load the image, apply deploy/k8s, wait for rollouts
#   2. smoke Job (8 clients) on 6 storage pods
#   3. scale storage to $SCALE pods, run the throughput benchmark as a Job
#   4. force-delete a storage pod while a smoke Job is running
#   5. force-delete the Raft leader pod and time the new leader
# Needs docker, kind, kubectl. On Linux, multi-node kind needs
# fs.inotify.max_user_instances >= 512.
set -euo pipefail
cd "$(dirname "$0")/.."
SCALE=${SCALE:-50}
CTX=kind-dfs
K="kubectl --context $CTX -n dfs"
M0=dfs-meta-0.dfs-meta.dfs.svc.cluster.local:8000
M1=dfs-meta-1.dfs-meta.dfs.svc.cluster.local:8000
M2=dfs-meta-2.dfs-meta.dfs.svc.cluster.local:8000
META=$M0,$M1,$M2
step() { echo; echo "=== $(date -u +%T) $*"; }

dfs() { # run the CLI inside a meta pod (the image is distroless; exec the binary)
  local pod=${POD:-dfs-meta-0}
  $K exec "$pod" -c meta -- /usr/local/bin/dfs --meta "$META" "$@"
}

run_job() { # name args-json-list
  local name=$1 args=$2
  $K delete job "$name" --ignore-not-found --wait=true >/dev/null
  cat <<EOF | $K apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: $name, namespace: dfs}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: dfs:dev
          imagePullPolicy: IfNotPresent
          command: [/usr/local/bin/dfs-bench]
          args: [$args]
EOF
}

wait_job() { # name timeout
  $K wait --for=condition=complete "job/$1" --timeout="$2" || { $K logs "job/$1" | tail -20; return 1; }
  $K logs "job/$1"
}

leader() { dfs status 2>/dev/null | awk '/^leader:/ {print $2}'; }

step "fresh cluster + deploy"
kind delete cluster --name dfs >/dev/null 2>&1 || true
kind create cluster --name dfs --config deploy/kind/cluster.yaml
docker build -q -t dfs:dev .
kind load docker-image dfs:dev --name dfs
kubectl --context $CTX apply -k deploy/k8s
for s in cassandra minio dfs-meta dfs-storage; do $K rollout status "statefulset/$s" --timeout=900s; done
kubectl --context $CTX get nodes
dfs status | sed -n '1,5p;7p'

step "smoke on 6 storage pods"
$K delete job dfs-smoke --ignore-not-found --wait=true >/dev/null
$K apply -f deploy/k8s/smoke-job.yaml >/dev/null
wait_job dfs-smoke 600s

step "scale storage to $SCALE pods"
$K scale statefulset dfs-storage --replicas="$SCALE"
$K rollout status statefulset/dfs-storage --timeout=1200s
for _ in $(seq 1 60); do dfs status | grep -q "$SCALE up" && break; sleep 2; done
dfs status | grep 'ring epoch'
$K get pods -l app=dfs-storage -o wide --no-headers | awk '{print $7}' | sort | uniq -c

step "throughput job on $SCALE storage pods"
run_job dfs-throughput "\"throughput\",\"--cluster\",\"$META\",\"--storage\",\"$SCALE\",\"--clients\",\"32\",\"--total\",\"1536MiB\",\"--download-for\",\"30s\",\"--repair=false\",\"--host-desc\",\"kind on one Linux host (20 cores): 1 control plane + 3 worker nodes, $SCALE storage pods\",\"--out\",\"/tmp/out\""
wait_job dfs-throughput 1200s | grep -vE ' bench/|cpu@'

step "storage pod crash during traffic"
$K delete job dfs-smoke --ignore-not-found --wait=true >/dev/null
$K apply -f deploy/k8s/smoke-job.yaml >/dev/null
sleep 8
victim=dfs-storage-$((SCALE / 3))
t0=$(date +%s.%N)
$K delete pod "$victim" --grace-period=0 --force 2>/dev/null
echo "force-deleted $victim"
wait_job dfs-smoke 600s
$K wait --for=condition=Ready "pod/$victim" --timeout=300s >/dev/null
t1=$(date +%s.%N)
echo "$victim back and Ready after $(echo "$t1 - $t0" | bc) s (same identity and volume)"
dfs status | grep 'ring epoch'

step "Raft leader pod crash"
old=$(leader)
case $old in dfs-meta-0) other=dfs-meta-1 ;; *) other=dfs-meta-0 ;; esac
echo "leader is $old; force-deleting it"
t0=$(date +%s.%N)
$K delete pod "$old" --grace-period=0 --force 2>/dev/null
until new=$(POD=$other leader) && [ -n "$new" ] && [ "$new" != "$old" ]; do sleep 0.05; done
t1=$(date +%s.%N)
echo "new leader $new seen after $(echo "$t1 - $t0" | bc) s (includes kubectl exec round trips)"
$K wait --for=condition=Ready "pod/$old" --timeout=300s >/dev/null
POD=$other dfs status | sed -n '1,5p'

step "done"
