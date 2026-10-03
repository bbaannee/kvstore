#!/usr/bin/env bash
# Starts a 3-node cluster with the LSM storage engine and snapshots, then changes its membership while it
# is running: adds a 4th and a 5th server, kills two nodes with SIGKILL (the cluster of 5 survives), and
# removes a server. Everything goes through the kvd and kvctl executables, as separate OS processes.
#
# Usage: scripts/demo_membership.sh [build-dir]      (default build directory: build)

set -u
BUILD="${1:-build}"
BASE_PORT="${BASE_PORT:-18001}"
DATA="$(mktemp -d)"

addr() { echo "127.0.0.1:$((BASE_PORT + $1 - 1))"; }
PEERS="1=$(addr 1),2=$(addr 2),3=$(addr 3)"
SERVERS="$(addr 1),$(addr 2),$(addr 3)"
declare -A PID
COMMON=(--engine lsm --snapshot-every 20 --quiet)

start_member() {  # a node of the initial cluster
  "$BUILD/kvd" --id "$1" --peers "$PEERS" --data "$DATA/node$1" "${COMMON[@]}" 2> "$DATA/node$1.log" &
  PID[$1]=$!
}
start_joiner() {  # a node that will be added later
  "$BUILD/kvd" --id "$1" --listen "$(addr "$1")" --join --data "$DATA/node$1" "${COMMON[@]}" 2> "$DATA/node$1.log" &
  PID[$1]=$!
}
restart() {  # restart a node from its own data (works for members and for joined servers alike)
  if [ "$1" -le 3 ]; then start_member "$1"; else start_joiner "$1"; fi
}
kill_node() { kill -9 "${PID[$1]}" 2> /dev/null; wait "${PID[$1]}" 2> /dev/null; }
kvctl() { "$BUILD/kvctl" --servers "$SERVERS" --timeout-ms 20000 "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
cleanup() {
  for id in 1 2 3 4 5; do kill "${PID[$id]:-0}" 2> /dev/null; done
  wait 2> /dev/null
  echo; echo "(node logs and data were in $DATA)"
}
trap cleanup EXIT

step "starting 3 nodes (LSM engine, snapshot every 20 entries)"
for id in 1 2 3; do start_member "$id"; done
for i in $(seq 1 60); do kvctl put "key$((i % 12))" "value-$i" > /dev/null; done
kvctl put name Bane
kvctl get name

step "adding server 4 while the cluster is running (it catches up from the leader, by snapshot)"
start_joiner 4
kvctl add-server 4 "$(addr 4)"
kvctl put added-4 "written after server 4 joined"

step "adding server 5; the cluster now has 5 members and survives 2 failures"
start_joiner 5
kvctl add-server 5 "$(addr 5)"
kvctl put added-5 "written after server 5 joined"

step "kill -9 nodes 1 and 2 (two of five): the cluster keeps working"
kill_node 1
kill_node 2
kvctl put survived "with 3 of 5 servers"
kvctl get survived
kvctl get name

step "bring nodes 1 and 2 back from their disks"
restart 1
restart 2
sleep 1
kvctl get added-4
kvctl get added-5

step "remove server 5 (and stop it)"
kvctl remove-server 5
kill_node 5
kvctl put after-removal "4 servers"
kvctl get after-removal

step "everything written along the way is still there"
kvctl get name
kvctl get survived
kvctl get key3
