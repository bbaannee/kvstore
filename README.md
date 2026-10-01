# kvstore

A key-value store in C++20, being built step by step into a **distributed, fault-tolerant
store replicated with the Raft consensus algorithm**.

**Current stage:** a single-node store with a checksummed, crash-safe write-ahead log.

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
src/cli/       command parser, REPL, main
tests/         GoogleTest suites
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
- [ ] Raft: leader election and log replication (tested in-process first)
- [ ] Deterministic simulation: partitions, crashes, message loss; linearizability checker
- [ ] Networking: binary RPC over TCP, multi-node cluster, client library
- [ ] Snapshots and log compaction
- [ ] LSM-tree storage engine (SSTables, compaction)
- [ ] Cluster membership changes
