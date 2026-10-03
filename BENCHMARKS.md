# Benchmarks

Produced by `kvbench` (Release build) on one machine: 3 `kvd` servers over loopback TCP, files on a real ext4 disk
(`fsync` ≈ 3 ms). Compare rows with each other, not with other machines.

```bash
cmake -B build-rel -DCMAKE_BUILD_TYPE=Release && cmake --build build-rel -j --target kvbench
mkdir -p ~/kvbench-scratch && ./build-rel/kvbench --dir ~/kvbench-scratch     # subcommands: wal | lsm | cluster, --quick
```

**Caveat: pick the directory carefully.** On many Linux systems `/tmp` is a RAM disk (tmpfs) where `fsync` is free.
Every durability number there would be meaningless, so `kvbench` warns when it detects tmpfs.

## Write-ahead log: what an fsync costs

| fsync policy | records/s | µs/record |
|---|---|---|
| every record | 277 | 3607.9 |
| every 10 records | 3,421 | 292.3 |
| every 100 records | 32,374 | 30.9 |
| never (page cache only) | 953,291 | 1.0 |

## LSM tree (400,000 puts of 100-byte values)

| workload | memtable | puts/s | MB/s | flushes | compactions | write amplification |
|---|---|---|---|---|---|---|
| sequential keys | 1 MB | 296,687 | 33.5 | 68 | 44 | 3.1x |
| sequential keys | 4 MB | 325,156 | 36.7 | 17 | 23 | 3.0x |
| sequential keys | 16 MB | 318,650 | 36.0 | 5 | 19 | 3.0x |
| random keys | 1 MB | 174,362 | 19.7 | 68 | 37 | 7.4x |
| random keys | 4 MB | 221,290 | 25.0 | 17 | 22 | 4.2x |
| random keys | 16 MB | 218,400 | 24.7 | 5 | 18 | 2.9x |

Random keys with a small memtable rewrite data the most; a bigger memtable absorbs more overwrites before they reach disk.

### Bloom filter (101 overlapping tables)

| bloom filter | present keys/s | ABSENT keys/s |
|---|---|---|
| 10 bits/key | 92,334 | 102,486 |
| none | 2,861 | 1,799 |

Full range scan: 2,349,015 entries/s.

## Cluster writes: group commit

| configuration | clients | ops/s | p50 ms | p99 ms | p99.9 ms |
|---|---|---|---|---|---|
| fsync, one per request | 1 | 153 | 6.23 | 11.74 | 37.05 |
| fsync, one per request | 8 | 179 | 45.23 | 62.48 | 74.29 |
| fsync, one per request | 32 | 173 | 186.21 | 227.95 | 228.00 |
| fsync, group commit | 1 | 161 | 6.14 | 8.61 | 12.23 |
| fsync, group commit | 8 | 450 | 16.97 | 27.48 | 43.24 |
| fsync, group commit | 32 | 1,604 | 20.34 | 26.24 | 60.19 |
| no fsync (RAM speed) | 1 | 3,640 | 0.26 | 0.52 | 0.87 |
| no fsync (RAM speed) | 32 | 12,765 | 2.40 | 4.82 | 15.52 |

Without group commit, throughput is pinned near 1 / fsync-time regardless of client count; with it, the clients that
arrive during one fsync share the next one.

## Cluster reads: ReadIndex

| configuration | clients | ops/s | p50 ms | p99 ms | p99.9 ms |
|---|---|---|---|---|---|
| read through the log | 1 | 159 | 6.13 | 8.07 | 46.45 |
| read through the log | 32 | 1,601 | 20.19 | 27.29 | 63.25 |
| ReadIndex (no log entry) | 1 | 4,730 | 0.19 | 0.41 | 0.74 |
| ReadIndex (no log entry) | 32 | 13,854 | 2.23 | 3.56 | 5.12 |

## Storage engine behind the cluster (32 clients, writes)

| engine | ops/s | p50 ms | p99 ms |
|---|---|---|---|
| memory | 1,549 | 20.85 | 27.66 |
| LSM tree | 1,391 | 23.19 | 29.89 |

The cluster is bound by fsync, not by the engine, so the two are close.
