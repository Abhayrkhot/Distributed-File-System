#!/usr/bin/env bash
# Runs one role on one Slurm node: backends | meta | storage.
#   node-run.sh <role> <name>
# Jobs find each other through files under $RUN (a shared filesystem):
#   $RUN/addr/<name>      this node's IP (written at start)
#   $RUN/backends.env     Cassandra/S3 endpoints (written by the backends role)
#   $RUN/kill/<name>      crash <name> with SIGKILL; it stays down until
#   $RUN/restart/<name>   appears
#   $RUN/stop             everything shuts down
set -uo pipefail
role=$1 name=$2
: "${RUN:?}" "${DFS_HOME:?}"
BIN=$DFS_HOME/bin
SIF=$DFS_HOME/sif
# Nodes can have several interfaces (Explorer's d-nodes: 10.97/16, 10.99/16
# and InfiniBand); only one network is shared by every node.
NET=${DFS_NET_PREFIX:-10.99.}
IP=""
for a in $(hostname -I); do case $a in "$NET"*) IP=$a; break ;; esac; done
[ -n "$IP" ] || IP=$(hostname -I | awk '{print $1}')
LOCAL=/tmp/dfs-${SLURM_JOB_ID:-x}-$name
mkdir -p "$LOCAL" "$RUN"/{addr,kill,restart,logs}
LOG=$RUN/logs/$name.log
log() { echo "$(date -u +%FT%T.%3NZ) [$name@$(hostname -s)] $*" >>"$LOG"; }
cleanup() { rm -rf "$LOCAL"; }
trap cleanup EXIT

# Compute nodes set an HTTP proxy; gRPC and the S3 client would send
# node-to-node traffic through it.
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

# Fixed ports collide with site services (9100 is node_exporter), so every
# process takes a free high port and publishes it in its address file.
pick_port() {
  local p
  for p in $(shuf -i 20000-60999 -n 100); do
    (echo >"/dev/tcp/127.0.0.1/$p") 2>/dev/null || { echo "$p"; return; }
  done
  return 1
}
publish() { echo "$*" >"$RUN/addr/$name.tmp" && mv "$RUN/addr/$name.tmp" "$RUN/addr/$name"; }
log "role=$role ip=$IP local=$LOCAL"

wait_file() { while [ ! -s "$1" ]; do [ -e "$RUN/stop" ] && exit 0; sleep 1; done; }
wait_port() { until (echo >"/dev/tcp/$1/$2") 2>/dev/null; do [ -e "$RUN/stop" ] && exit 0; sleep 2; done; }

# supervise <cmd...>: run cmd; obey kill/restart/stop files.
supervise() {
  "$@" >>"$LOG" 2>&1 &
  local pid=$!
  while :; do
    if [ -e "$RUN/stop" ]; then kill -TERM "$pid" 2>/dev/null; wait "$pid"; return 0; fi
    if [ -e "$RUN/kill/$name" ]; then
      kill -KILL "$pid" 2>/dev/null; wait "$pid" 2>/dev/null
      rm -f "$RUN/kill/$name"; log "SIGKILLed"
      while [ ! -e "$RUN/restart/$name" ] && [ ! -e "$RUN/stop" ]; do sleep 0.2; done
      [ -e "$RUN/stop" ] && return 0
      rm -f "$RUN/restart/$name"; log "restarting"
      "$@" >>"$LOG" 2>&1 &
      pid=$!
    fi
    if ! kill -0 "$pid" 2>/dev/null; then wait "$pid"; log "exited with $?"; return 1; fi
    sleep 0.1
  done
}

case $role in
backends)
  mport=$(pick_port)
  mkdir -p "$LOCAL"/cass/{data,logs} "$LOCAL/minio"
  apptainer run --writable-tmpfs \
    --bind "$LOCAL/cass/data:/var/lib/cassandra,$LOCAL/cass/logs:/var/log/cassandra" \
    --env "MAX_HEAP_SIZE=4G,HEAP_NEWSIZE=800M,CASSANDRA_LISTEN_ADDRESS=$IP,CASSANDRA_BROADCAST_ADDRESS=$IP,CASSANDRA_RPC_ADDRESS=0.0.0.0,CASSANDRA_BROADCAST_RPC_ADDRESS=$IP,CASSANDRA_SEEDS=$IP" \
    "$SIF/cassandra.sif" >>"$RUN/logs/cassandra.log" 2>&1 &
  cass=$!
  apptainer run --bind "$LOCAL/minio:/data" \
    --env "MINIO_ROOT_USER=dfsminio,MINIO_ROOT_PASSWORD=dfsminio-secret" \
    "$SIF/minio.sif" server /data --address ":$mport" --console-address ":$(pick_port)" >>"$RUN/logs/minio.log" 2>&1 &
  minio=$!
  publish "$IP 9042 $mport"
  wait_port "$IP" "$mport"; log "minio up on $mport"
  wait_port "$IP" 9042; log "cassandra up"
  cat >"$RUN/backends.env.tmp" <<EOF
export DFS_CASSANDRA=$IP:9042
export DFS_S3_ENDPOINT=$IP:$mport
export DFS_S3_ACCESS_KEY=dfsminio
export DFS_S3_SECRET_KEY=dfsminio-secret
export DFS_S3_BUCKET=dfs-chunks
export DFS_S3_ENCRYPTION_KEY=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
export DFS_KEYSPACE=dfs
EOF
  mv "$RUN/backends.env.tmp" "$RUN/backends.env"
  while [ ! -e "$RUN/stop" ]; do sleep 2; done
  kill "$cass" "$minio" 2>/dev/null; wait
  ;;
meta)
  wait_file "$RUN/backends.env"; source "$RUN/backends.env"
  rport=$(pick_port) gport=$(pick_port)
  publish "$IP $rport $gport"
  for m in meta-0 meta-1 meta-2; do wait_file "$RUN/addr/$m"; done
  peers=""
  for m in meta-0 meta-1 meta-2; do read -r a r g <"$RUN/addr/$m"; peers="${peers:+$peers,}$m=$a:$r=$a:$g"; done
  supervise "$BIN/dfs-meta" --id "$name" --raft-bind "0.0.0.0:$rport" --raft-advertise "$IP:$rport" \
    --grpc-bind "0.0.0.0:$gport" --grpc-advertise "$IP:$gport" --peers "$peers" --bootstrap \
    --data-dir "$LOCAL/meta" --timing "${DFS_TIMING:-fast}" --dead-after "${DFS_DEAD_AFTER:-5s}" \
    --repair-interval 1h --log-level info --raft-log-level warn
  ;;
storage)
  wait_file "$RUN/backends.env"; source "$RUN/backends.env"
  port=$(pick_port)
  publish "$IP $port"
  metas=$("$DFS_HOME/deploy/slurm/meta-addrs.sh" "$RUN")
  supervise "$BIN/dfs-storage" --id "$name" --bind "0.0.0.0:$port" --advertise "$IP:$port" \
    --data-dir "$LOCAL/data" --meta "$metas" --backup-workers 4 --log-level warn
  ;;
*) echo "unknown role $role" >&2; exit 2 ;;
esac
