#!/usr/bin/env bash
# Starts a 3-node cluster as three separate processes on localhost, then
# kills each node with SIGKILL in turn (no clean shutdown, no goodbye) while
# using the cluster through the kvctl command-line client.
#
# Usage: scripts/demo_cluster.sh [build-dir]      (default build directory: build)

set -u
BUILD="${1:-build}"
BASE_PORT="${BASE_PORT:-17001}"
DATA="$(mktemp -d)"

PEERS="1=127.0.0.1:${BASE_PORT},2=127.0.0.1:$((BASE_PORT + 1)),3=127.0.0.1:$((BASE_PORT + 2))"
SERVERS="127.0.0.1:${BASE_PORT},127.0.0.1:$((BASE_PORT + 1)),127.0.0.1:$((BASE_PORT + 2))"
declare -A PID

start_node() {
  "$BUILD/kvd" --id "$1" --peers "$PEERS" --data "$DATA/node$1" 2> "$DATA/node$1.log" &
  PID[$1]=$!
}
kill_node() {
  kill -9 "${PID[$1]}" 2> /dev/null
  wait "${PID[$1]}" 2> /dev/null
}
kvctl() { "$BUILD/kvctl" --servers "$SERVERS" --timeout-ms 20000 "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
cleanup() {
  for id in 1 2 3; do kill "${PID[$id]:-0}" 2> /dev/null; done
  wait 2> /dev/null
  echo; echo "(node logs and data were in $DATA)"
}
trap cleanup EXIT

step "starting 3 nodes (data in $DATA)"
for id in 1 2 3; do start_node "$id"; done

step "writing and reading through whichever node is leader"
kvctl put name Bane
kvctl put city "Novi Sad"
kvctl get name

for victim in 1 2 3; do
  step "kill -9 node $victim (might be the leader), then keep using the cluster"
  kill_node "$victim"
  kvctl put "written-without-node-$victim" "still works"
  kvctl get "written-without-node-$victim"
  kvctl get name

  step "restart node $victim; it recovers its Raft log from disk"
  start_node "$victim"
  sleep 1
done

step "everything written along the way is still there"
kvctl get name
kvctl get city
for victim in 1 2 3; do kvctl get "written-without-node-$victim"; done

step "delete, and check that a missing key reports so"
kvctl del city
kvctl get city
