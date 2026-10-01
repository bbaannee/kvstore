# kvstore

A key-value store in C++20, being built step by step into a **distributed, fault-tolerant
store replicated with the Raft consensus algorithm**.

**Current stage:** a checksummed, crash-safe single-node store, plus a Raft consensus core
(leader election + log replication) tested as whole clusters under random failures. Next:
running Raft over a real network with on-disk persistence.

## How it works

```
 put/del ──▶ LogWriter ──▶ data.log (append + fsync) ──▶ in-memory map
                                   │
 startup ──▶ LogReader ────────────┘ verify checksums, replay, cut off a torn tail
```

- **Write-ahead log.** Every change is appended to `data.log` and `fsync`'ed *before* it is
  applied to memory, so a change that returned `OK` survives a crash or a power loss.
- **Binary, checksummed records.** Each record is framed as
  `[length][crc32c(length)][crc32c(payload)][payload]` (little-endian u32s). The length has its
  own checksum, so a damaged length can never be mistaken for a half-written record.
- **Torn write vs. corruption.** On startup the log is replayed and every checksum verified. A
  damaged record **at the end** (or a zero-filled tail) is what a crash leaves behind: it's cut
  off. A damaged record **anywhere else** means the disk corrupted data: the store refuses to open
  and reports the byte offset instead of silently dropping data.
- **Tested at every crash point and every bit flip.** `RecoversFromCrashAtEveryByte` cuts the log
  at every byte offset; `BitFlipBeforeTheLastRecordIsCorruption` flips a bit in every byte and
  checks each one is reported as corruption.

## Raft

`src/raft/` implements Raft as a **pure state machine**: `RaftNode` never touches the network,
the disk or the clock.

```
 tick()      ─┐                     ┌─▶ Ready.hardState / entries  → persist (fsync)
 step(msg)   ─┼─▶   RaftNode   ─────┼─▶ Ready.messages             → send
 propose(x)  ─┘                     └─▶ Ready.committedEntries     → apply
```

- **Leader election** with randomized timeouts, one vote per term, and the up-to-date-log vote
  restriction.
- **Log replication** with the `prevLogIndex`/`prevLogTerm` consistency check, conflict
  truncation, a fast-backtracking reject hint, and batching.
- **Commit rule** counts replicas only for current-term entries, and every new leader appends a
  no-op (Raft paper, Figure 8).

**Testing.** Because the node is deterministic, `tests/raft/test_cluster.h` runs a whole cluster
in one process with a fake network (random reordering, message loss, partitions) and fake disks
(crash/restart). It checks **Election Safety** and **State Machine Safety** after every step.
`SafeUnderRandomFailures` runs 30 seeds × 300 steps of random chaos. Mutation testing (breaking
each core rule on purpose) confirms the suite catches every one of them.

## Layout

```
src/common/    building blocks
  coding       little-endian fixed-width integers
  crc32c       CRC-32C checksum (table-driven)
src/storage/   storage engine (knows nothing about the CLI)
  log_record   record payload: encode / decode
  log_format   on-disk framing: header + checksums
  log_writer   durable appends (write loop + fsync)
  log_reader   verified sequential reads, torn-vs-corrupt rules
  kv_store     KvStore: log + in-memory map + recovery
  file_util    RAII file descriptor, fsync / truncate helpers
src/raft/      Raft consensus (pure state machine)
  types        NodeId, Term, Index, Entry, HardState
  message      RequestVote / AppendEntries and their responses
  raft_log     in-memory log with a sentinel at index 0
  raft_node    election, replication, commit; Ready output
src/cli/       command parser, REPL, main
tests/         GoogleTest suites
  raft/        unit tests, in-process cluster harness, chaos tests
```

## Build and run

Requires Linux (or WSL), a C++20 compiler, CMake and GoogleTest:

```bash
sudo apt install -y build-essential cmake libgtest-dev
cmake -B build
cmake --build build
ctest --test-dir build --output-on-failure
./build/kv            # uses ./data.log; or: ./build/kv path/to/log
```

With sanitizers: `cmake -B build-asan -DKV_SANITIZE=ON && cmake --build build-asan`.

```
> put msg hello world
OK
> get msg
hello world
> del msg
OK
```

## Roadmap

- [x] Single-node store with a crash-safe write-ahead log
- [x] Checksums to tell a torn write apart from corruption; binary record format
- [x] Raft: leader election and log replication (tested in-process first)
- [x] Deterministic simulation: partitions, crashes, message loss, safety monitors
- [ ] Raft log + state on disk; Raft driving the KvStore
- [ ] Linearizability checker for client operations
- [ ] Networking: binary RPC over TCP, multi-node cluster, client library
- [ ] Snapshots and log compaction
- [ ] LSM-tree storage engine (SSTables, compaction)
- [ ] Cluster membership changes
