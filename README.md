# kvstore

A **distributed, fault-tolerant key-value store** in C++20, built from scratch: its own Raft consensus,
its own LSM-tree storage engine, its own binary network protocol, and a test suite that kills servers,
cuts the network and checks that what clients saw is **linearizable**.

```bash
scripts/demo_cluster.sh       # 3 real processes; kill -9 each node in turn while using the cluster
scripts/demo_membership.sh    # LSM engine + snapshots; add servers 4 and 5, kill 2 of 5, remove one
```

```
 kvctl ──TCP──▶ ┌──────────────────────────── kvd (one process) ───────────────────────────┐ ◀──TCP──▶ other kvd
 (client lib)   │  epoll event loop ─▶ FrameDecoder ─▶ decodePacket                         │
                │      ├▶ Raft messages ─────────────▶ ┌────────────┐                       │
                │      ├▶ client requests ───────────▶ │  RaftNode  │ pure state machine:   │
                │      ├▶ membership requests ───────▶ │ (election, │ no network, disk or   │
                │      └▶ hello (peer addresses)       │ replication│ clock access          │
                │                                      │ snapshots, │                       │
                │   one fsync per loop iteration       │ ReadIndex) │                       │
                │   (group commit)                     └─────┬──────┘                       │
                │                          Ready{ persist · send · apply · answer }         │
                │      ┌─────────────────────┐     ┌─────────┴──────────┐                   │
                │      │ RaftStorage         │     │ KvStateMachine     │                   │
                │      │ write-ahead log     │     │ sessions, dedup    │                   │
                │      │ + snapshot, atomic  │     │ ┌───────┐ ┌──────┐ │                   │
                │      │ compaction          │     │ │memory │ │ LSM  │ │                   │
                │      └─────────────────────┘     │ └───────┘ │ tree │ │                   │
                │                                  └───────────┴──────┴─┘                   │
                └───────────────────────────────────────────────────────────────────────────┘
```

## What it does

| Area | What's in it |
|---|---|
| **Consensus** | Raft: leader election with randomized timeouts, log replication with pipelining and byte-limited batches, the Figure-8-safe commit rule, **snapshots with chunked `InstallSnapshot`**, **single-server membership changes** (add / remove, including removing the leader), **ReadIndex** linearizable reads without a log entry, **check-quorum** (a cut-off leader steps down), **leader stickiness** (a removed server can't depose a healthy leader) |
| **Durability** | Checksummed write-ahead log (CRC-32C, torn-write vs corruption detection), `fsync` discipline including directory syncs, atomic file replacement for compaction, crash recovery tested at every byte |
| **Storage engine** | An **LSM tree**: memtable, SSTables (checksummed blocks, index, Bloom filter, footer), manifest, leveled compaction with tombstone handling, crash-safe flush/compaction. Pluggable under the replicated state machine, next to an in-memory engine |
| **Network** | Binary protocol over TCP, single-threaded **epoll** event loop, framing and bounds-checked decoding, reconnects, connect-on-demand, partial writes, bounded buffers |
| **Clients** | Leader discovery from hints, failover, **exactly-once** execution through `(clientId, seq)` sessions (bounded, deterministic eviction), exponential backoff |
| **Performance** | **Group commit**: one fsync per event-loop iteration for all the requests in it |
| **Verification** | A **linearizability checker** (Wing & Gong search with memoization) run on histories from chaos tests; whole clusters in one process with a deterministic fake network, crashes and partitions; mutation testing; ASan/UBSan and ThreadSanitizer clean |

## How correctness is checked

The strongest claim a replicated store can make is **linearizability**: it behaves as if it were a single
machine, however many servers crash or restarts and retries happen behind the scenes. `src/verify/` decides
whether a recorded history of client operations satisfies it, and the tests feed it three kinds of history:

1. **In-process chaos.** Whole clusters of real nodes with real files, a fake network (random reordering, loss,
   partitions) and random crashes and restarts; clients whose operations time out and stay *unknown*. Every
   seed's history must be linearizable, including with ReadIndex reads, snapshots and the LSM engine on.
2. **Real sockets, real threads.** Four client threads, three `kvd` event loops on TCP ports, servers killed and
   restarted from disk while the clients run (~20,000 operations per run, memory and LSM engines).
3. **A history built to be wrong.** A stale read from a deposed leader: the checker has to reject it, and the same
   scenario through ReadIndex has to refuse to answer.

Beyond that: crash-at-every-byte tests for every file format, bit-flip tests for every checksum, a model-based
randomized test of the LSM tree against `std::map`, an equivalence test between the two storage engines, and
**mutation testing**: breaking each core rule on purpose in a throwaway copy of the code and confirming the suite
notices. The tests found real bugs along the way; the interesting ones are listed under *Bugs the tests found*.

## Layout

```
src/common/    little-endian integers, bounds-checked ByteReader, CRC-32C (slicing-by-8)
src/storage/   the checksummed write-ahead log: record format, writer (fsync), reader (torn vs corrupt)
src/lsm/       the LSM tree: bloom_filter, memtable, sstable, cursor (merging), lsm_tree (WAL, manifest, compaction)
src/raft/      types, messages, raft_log (with a snapshot base), raft_node (the algorithm, no I/O),
               raft_storage (term, vote, log and snapshot in one atomically-replaced file), codecs
src/server/    command, kv_storage (memory / LSM), kv_state_machine (sessions, snapshots),
               replicated_node (Raft + disk + state machine), node_server (epoll loop), kvd_main
src/net/       sockets, frame decoder, packet codec, size limits
src/client/    client library and the kvctl executable
src/verify/    history recorder and the linearizability checker
src/cli/       the original single-node `kv` REPL
bench/         kvbench
scripts/       the two demos
tests/         raft/ server/ net/ lsm/ verify/ + harnesses (in-process cluster, live TCP cluster)
```

## Build and run

Requires Linux (or WSL), a C++20 compiler, CMake and GoogleTest:

```bash
sudo apt install -y build-essential cmake libgtest-dev
cmake -B build && cmake --build build -j
ctest --test-dir build --output-on-failure      # the whole suite, ~75 s
```

Sanitizers (each in its own build directory): `-DKV_SANITIZE=ON` (AddressSanitizer + UBSan), `-DKV_TSAN=ON` (ThreadSanitizer).

### A cluster

One command per terminal (or run `scripts/demo_cluster.sh`):

```bash
PEERS=1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003
./build/kvd --id 1 --peers $PEERS --data /tmp/kv1 --engine lsm --snapshot-every 1000
./build/kvd --id 2 --peers $PEERS --data /tmp/kv2 --engine lsm --snapshot-every 1000
./build/kvd --id 3 --peers $PEERS --data /tmp/kv3 --engine lsm --snapshot-every 1000

SERVERS=127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003
./build/kvctl --servers $SERVERS put name Bane
./build/kvctl --servers $SERVERS get name
./build/kvctl --servers $SERVERS del name
```

Changing the membership while it runs: start the new server as a *joiner*, then tell the cluster:

```bash
./build/kvd --id 4 --listen 127.0.0.1:7004 --join --data /tmp/kv4 --engine lsm &
./build/kvctl --servers $SERVERS add-server 4 127.0.0.1:7004
./build/kvctl --servers $SERVERS remove-server 3
```

`kvd` flags: `--engine memory|lsm`, `--snapshot-every N`, `--tick-ms`, `--no-group-commit`, `--no-read-index`,
`--no-fsync` (unsafe, for experiments), `--quiet`. A joiner is remembered in its data directory, so it
restarts as a joiner without repeating the flag.

## Benchmarks

`./build-release/kvbench --dir <a directory on a real disk>` (see [BENCHMARKS.md](BENCHMARKS.md) for a full run,
and for why the directory matters: `/tmp` can be a RAM disk where `fsync` is free). Highlights from one machine,
3 servers over loopback with real fsyncs:

| | before | after |
|---|---|---|
| fsync per write → **group commit** (32 clients) | 173 writes/s, p50 186 ms | **1,604 writes/s**, p50 20 ms |
| read through the log → **ReadIndex** (32 clients) | 1,601 reads/s | **13,854 reads/s** |
| LSM lookups of absent keys, no filter → **Bloom filter** | 1,799/s | **102,486/s** |

## Design decisions and trade-offs

- **Raft is a pure state machine** (`RaftNode`): input is `tick()`, `step(msg)`, `propose()`; output is a `Ready`
  to persist, send and apply, in that order. It never touches a socket, a file or a clock, which is what lets a
  whole cluster run in one process under a seed, with every failure scripted and reproducible.
- **Snapshots live inside the Raft log file**, replaced atomically with one `rename`. An earlier design with a
  second file could leave a snapshot and an older log that contradicted each other after a crash.
- **Single-server membership changes**, one at a time, each committed before the next, with the configuration
  stored in the log (so a joining server learns it by replication) and effective as soon as it is *appended*.
- **ReadIndex** instead of lease reads: no clock assumptions. A read waits for a majority to acknowledge a heartbeat
  round that began after it arrived.
- **Group commit** by deferring the node's persist step to the end of the event-loop iteration, not by adding threads.
- **The state machine is derived data.** With the LSM engine it also persists its own applied index atomically with
  each batch of writes, so a restart doesn't replay the log; the Raft log is discarded only up to a point the state
  machine has synced.
- **Every request is bounded** (keys 4 KB, values 1 MB) because everything runs on one thread per node: one huge request
  stalls heartbeats and triggers elections.

## Known limitations

- Snapshots are whole-state blobs held in memory and stored in one record, so state size is limited to about 128 MB per
  snapshot. A real system would stream chunks from a consistent LSM checkpoint instead.
- Membership changes add voting members directly. A new server that is far behind counts toward the quorum while it
  catches up (the Raft thesis suggests non-voting learners first). Joiners vote, so they can't deadlock the cluster, but
  availability dips until they catch up.
- No TLS or authentication; a node trusts the peers that connect to it.
- Compaction runs synchronously inside writes (no background thread), so a write that triggers a big compaction stalls.
- The client session table is bounded (LRU, 10,000 clients); a client idle for long enough is forgotten and a late retry
  would be executed again.
- Linearizability is checked on key-value operations only (no transactions, no range reads).
- Only loopback networking was exercised; the in-process harness covers partitions and reordering for the protocol, but not
  for the socket layer.

## Bugs the tests found

The suite earned its keep. A few of the more instructive ones, all fixed:

- **Retry storm.** A 12 MB value made a request slower than the client's timeout; each retry was another 12 MB for the
  cluster. Fixes: request size limits, client backoff, a byte limit per `AppendEntries` (64 × 1 MB would exceed the frame
  limit and stall replication forever), pipelining instead of re-sending unacknowledged entries, a faster CRC.
- **Leader churn from head-of-line blocking.** Heartbeats stuck behind a big message on the same TCP stream looked like a
  dead leader (the classic *broadcast time ≪ election timeout* rule).
- **Joiner deadlock.** A just-added server counts toward the majority but refused to vote because it didn't yet know it was
  a member, so no leader could be elected to tell it. Voting now depends on the log only; the candidate decides whose votes count.
- **A frozen clock.** A non-member's election timer didn't advance, so "I heard a leader recently" never expired and, with
  leader stickiness, it ignored every vote request forever.
- **No way to answer a stranger.** Replies went only over outgoing connections to known members, so a new server's vote was
  dropped. Servers now connect on demand to anyone they have a message for and an address for.
- **Two files, two truths.** Snapshot and log in separate files could disagree after a crash; they now share one file.
- **A harness bug that hid a test:** two clusters in one test shared a temp directory and silently wiped each other.
- **Compiler warnings caught undefined behaviour** in tests (references into destroyed temporaries).
