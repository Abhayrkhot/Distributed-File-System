#!/usr/bin/env bash
# Run Cassandra and MinIO natively (Homebrew) for local development and
# benchmarks. State lives under .local/ in the repo. Ports are shifted off the
# defaults because macOS AirPlay holds :7000.
#
#   scripts/dev-deps.sh up      start both and wait until they accept clients
#   scripts/dev-deps.sh down    stop both
#   scripts/dev-deps.sh status
#   scripts/dev-deps.sh wipe    stop and delete .local/
#
# Environment printed by `up` (also what the tests and benchmarks read):
#   DFS_CASSANDRA=127.0.0.1:19042
#   DFS_S3_ENDPOINT=127.0.0.1:19000  DFS_S3_ACCESS_KEY / DFS_S3_SECRET_KEY / DFS_S3_BUCKET
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL="$ROOT/.local"
CASS_PORT="${CASS_PORT:-19042}"
CASS_STORAGE_PORT="${CASS_STORAGE_PORT:-17000}"
CASS_JMX_PORT="${CASS_JMX_PORT:-17199}"
CASS_HEAP="${CASS_HEAP:-1G}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-19001}"
MINIO_USER="${MINIO_USER:-dfsminio}"
MINIO_PASS="${MINIO_PASS:-dfsminio-secret}"
BUCKET="${DFS_S3_BUCKET:-dfs-chunks}"

brew_cassandra_include() {
  local prefix
  prefix="$(brew --prefix cassandra)"
  echo "$prefix/share/cassandra/cassandra.in.sh"
}

setup_cassandra_conf() {
  local conf="$LOCAL/cassandra/conf"
  [[ -f "$conf/cassandra.yaml" ]] && return
  mkdir -p "$conf" "$LOCAL/cassandra/data" "$LOCAL/cassandra/log"
  cp -R "$(brew --prefix)/etc/cassandra/." "$conf/"
  sed -i '' \
    -e "s/^storage_port: .*/storage_port: $CASS_STORAGE_PORT/" \
    -e "s/^ssl_storage_port: .*/ssl_storage_port: $((CASS_STORAGE_PORT + 1))/" \
    -e "s/^native_transport_port: .*/native_transport_port: $CASS_PORT/" \
    -e "s/- seeds: \"127.0.0.1:7000\"/- seeds: \"127.0.0.1:$CASS_STORAGE_PORT\"/" \
    -e "s/^listen_address: .*/listen_address: 127.0.0.1/" \
    -e "s/^rpc_address: .*/rpc_address: 127.0.0.1/" \
    "$conf/cassandra.yaml"
  sed -i '' -e "s/^JMX_PORT=\"7199\"/JMX_PORT=\"$CASS_JMX_PORT\"/" "$conf/cassandra-env.sh"
  cat >"$LOCAL/cassandra/cassandra.in.sh" <<EOF
. "$(brew_cassandra_include)"
cassandra_storagedir="$LOCAL/cassandra/data"
EOF
}

cassandra_up() {
  setup_cassandra_conf
  if nc -z 127.0.0.1 "$CASS_PORT" 2>/dev/null; then echo "cassandra: already listening on $CASS_PORT"; return; fi
  echo "cassandra: starting (heap $CASS_HEAP) ..."
  CASSANDRA_CONF="$LOCAL/cassandra/conf" \
  CASSANDRA_INCLUDE="$LOCAL/cassandra/cassandra.in.sh" \
  CASSANDRA_LOG_DIR="$LOCAL/cassandra/log" \
  MAX_HEAP_SIZE="$CASS_HEAP" HEAP_NEWSIZE=256M \
    nohup cassandra -f -p "$LOCAL/cassandra/pid" >"$LOCAL/cassandra/log/stdout.log" 2>&1 &
  echo $! >"$LOCAL/cassandra/pid"
  for _ in $(seq 1 180); do
    if nc -z 127.0.0.1 "$CASS_PORT" 2>/dev/null; then echo "cassandra: ready on 127.0.0.1:$CASS_PORT"; return; fi
    sleep 1
  done
  echo "cassandra: did not start; see $LOCAL/cassandra/log/stdout.log" >&2
  return 1
}

minio_up() {
  mkdir -p "$LOCAL/minio/data"
  if nc -z 127.0.0.1 "$MINIO_PORT" 2>/dev/null; then echo "minio: already listening on $MINIO_PORT"; return; fi
  echo "minio: starting ..."
  MINIO_ROOT_USER="$MINIO_USER" MINIO_ROOT_PASSWORD="$MINIO_PASS" \
    nohup minio server --quiet --address "127.0.0.1:$MINIO_PORT" \
      --console-address "127.0.0.1:$MINIO_CONSOLE_PORT" "$LOCAL/minio/data" \
      >"$LOCAL/minio/stdout.log" 2>&1 &
  echo $! >"$LOCAL/minio/pid"
  for _ in $(seq 1 60); do
    if nc -z 127.0.0.1 "$MINIO_PORT" 2>/dev/null; then echo "minio: ready on 127.0.0.1:$MINIO_PORT"; return; fi
    sleep 0.5
  done
  echo "minio: did not start; see $LOCAL/minio/stdout.log" >&2
  return 1
}

stop_pidfile() {
  local name="$1" pidfile="$2"
  if [[ -f "$pidfile" ]]; then
    local pid; pid="$(cat "$pidfile")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"; echo "$name: stopped ($pid)"
    fi
    rm -f "$pidfile"
  fi
}

print_env() {
  cat <<EOF
export DFS_CASSANDRA=127.0.0.1:$CASS_PORT
export DFS_S3_ENDPOINT=127.0.0.1:$MINIO_PORT
export DFS_S3_ACCESS_KEY=$MINIO_USER
export DFS_S3_SECRET_KEY=$MINIO_PASS
export DFS_S3_BUCKET=$BUCKET
EOF
}

# Linux (no Homebrew): run both backends in Docker with the same host ports.
if ! command -v brew >/dev/null 2>&1 && command -v docker >/dev/null 2>&1; then
  COMPOSE=(docker compose -f "$ROOT/deploy/dev/compose.yaml")
  export CASS_PORT CASS_STORAGE_PORT CASS_JMX_PORT CASS_HEAP MINIO_PORT MINIO_CONSOLE_PORT MINIO_USER MINIO_PASS
  cassandra_up() {
    "${COMPOSE[@]}" up -d cassandra
    for _ in $(seq 1 240); do
      if nc -z 127.0.0.1 "$CASS_PORT" 2>/dev/null; then echo "cassandra: ready on 127.0.0.1:$CASS_PORT"; return; fi
      sleep 1
    done
    echo "cassandra: did not start; docker compose logs cassandra" >&2; return 1
  }
  minio_up() {
    "${COMPOSE[@]}" up -d minio
    for _ in $(seq 1 60); do
      if nc -z 127.0.0.1 "$MINIO_PORT" 2>/dev/null; then echo "minio: ready on 127.0.0.1:$MINIO_PORT"; return; fi
      sleep 0.5
    done
    echo "minio: did not start" >&2; return 1
  }
  case "${1:-}" in
    down) "${COMPOSE[@]}" stop; exit 0 ;;
    wipe) "${COMPOSE[@]}" down -v; rm -rf "$LOCAL"; echo "removed docker volumes"; exit 0 ;;
  esac
fi

case "${1:-}" in
  up) minio_up; cassandra_up; print_env ;;
  down) stop_pidfile minio "$LOCAL/minio/pid"; stop_pidfile cassandra "$LOCAL/cassandra/pid" ;;
  status)
    nc -z 127.0.0.1 "$CASS_PORT" 2>/dev/null && echo "cassandra: up ($CASS_PORT)" || echo "cassandra: down"
    nc -z 127.0.0.1 "$MINIO_PORT" 2>/dev/null && echo "minio: up ($MINIO_PORT)" || echo "minio: down" ;;
  env) print_env ;;
  wipe) "$0" down; rm -rf "$LOCAL"; echo "removed $LOCAL" ;;
  *) echo "usage: $0 up|down|status|env|wipe" >&2; exit 2 ;;
esac
