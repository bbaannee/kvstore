#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "lsm/cursor.h"
#include "lsm/memtable.h"
#include "lsm/sstable.h"
#include "storage/log_writer.h"

namespace kv::lsm {

struct Options {
    std::string dir;  // everything lives in this directory

    std::size_t memtableBytes = 4 * 1024 * 1024;  // flush to an SSTable when the memtable reaches this size
    std::size_t blockBytes = 4096;                // data block size inside tables
    int bloomBitsPerKey = 10;                     // ~1% false positives

    // Compaction. Level 0 holds fresh flushes (tables that may overlap each
    // other). When it has `l0CompactionTrigger` tables they are merged into
    // level 1. Level n >= 1 may hold up to level1MaxBytes * multiplier^(n-1);
    // beyond that, one of its tables is merged down into level n + 1.
    std::size_t l0CompactionTrigger = 4;
    std::size_t level1MaxBytes = 10 * 1024 * 1024;
    int levelMultiplier = 10;
    std::size_t targetFileBytes = 2 * 1024 * 1024;  // compaction cuts its output into tables of about this size
    int maxLevels = 7;

    // fsync the write-ahead log on every write. If false, the caller must call
    // sync() at the points where it needs durability (the replicated node does,
    // before it lets Raft discard the log entries behind a snapshot).
    bool syncWal = true;
    bool syncTables = true;  // fsync new tables and the manifest (false only in tests that don't need it)
};

// Several writes applied together: after a crash either all of them are
// there, or none.
class WriteBatch {
public:
    struct Op {
        bool erase = false;
        std::string key;
        std::string value;
    };

    void put(std::string key, std::string value) { ops_.push_back(Op{false, std::move(key), std::move(value)}); }
    void erase(std::string key) { ops_.push_back(Op{true, std::move(key), {}}); }
    void clear() { ops_.clear(); }
    bool empty() const { return ops_.empty(); }
    std::size_t size() const { return ops_.size(); }
    const std::vector<Op>& ops() const { return ops_; }

private:
    std::vector<Op> ops_;
};

struct Stats {
    std::size_t memtableBytes = 0;
    std::vector<std::size_t> tablesPerLevel;
    std::vector<std::uint64_t> bytesPerLevel;
    std::uint64_t flushes = 0;
    std::uint64_t compactions = 0;
    std::uint64_t bytesWrittenByFlush = 0;       // table bytes written when memtables were flushed
    std::uint64_t bytesWrittenByCompaction = 0;  // table bytes written by compaction (the rewriting an LSM tree pays for)
};

struct RecoveryInfo {
    std::size_t tablesLoaded = 0;
    std::size_t walBatchesReplayed = 0;
    bool truncatedTornTail = false;  // the log ended with a half-written record, which was cut off
};

// A log-structured merge tree: a key-value database built for fast writes.
//
//   write ──▶ write-ahead log (durable) ──▶ memtable (sorted, in memory)
//                                               │ full
//                                               ▼ flush
//   level 0   [table][table][table]      newest, may overlap each other
//   level 1   [table][table][table]...   sorted, no overlaps; ~10x bigger than level 0
//   level 2   [table][table]...          ~10x bigger again
//                    ▲ compaction merges tables from a level into the next one
//
// Why this design: a write is one sequential append to the log plus an
// insert into a sorted structure in memory: no random disk I/O, so writes
// are fast. The price is paid on reads (a key may be in the memtable or in
// several tables: newest first, and Bloom filters skip tables that can't have
// it) and in the background work of compaction (rewriting tables to keep the
// number of places to look small, and to drop overwritten values and
// deleted keys for good).
//
// Files in the directory:
//   MANIFEST         which tables exist, at which level, and which log is current
//                    (replaced atomically with a rename whenever that changes)
//   wal-NNNNNN.log   the write-ahead log of the current memtable
//   NNNNNN.sst       the tables
//
// Not thread-safe: use it from one thread (the replicated node's event loop).
// Compaction runs synchronously inside write calls.
class LsmTree {
public:
    // Opens the database in options.dir, creating it if needed. Recovery:
    // load the tables listed in the manifest, delete files that aren't part of
    // the database (leftovers of a crash), replay the write-ahead log into a
    // fresh memtable. Throws CorruptTableError / CorruptLogError for damage a
    // crash can't explain.
    explicit LsmTree(Options options);
    ~LsmTree();
    LsmTree(const LsmTree&) = delete;
    LsmTree& operator=(const LsmTree&) = delete;

    void put(std::string key, std::string value);
    void erase(std::string key);
    void write(const WriteBatch& batch);

    std::optional<std::string> get(std::string_view key) const;

    // Entries with start <= key < end (no upper bound if `end` is empty),
    // at most `limit`, in key order.
    std::vector<std::pair<std::string, std::string>> scan(std::string_view start, std::string_view end = {},
                                                          std::size_t limit = SIZE_MAX) const;

    // Calls `fn` for every live entry in key order.
    void forEach(const std::function<void(const std::string&, const std::string&)>& fn) const;

    // The same for entries with start <= key < end (no upper bound if `end` is
    // empty), without collecting them in memory first.
    void forEachInRange(std::string_view start, std::string_view end,
                        const std::function<void(const std::string&, const std::string&)>& fn) const;

    // Forces the log to disk (see Options::syncWal).
    void sync();

    // Writes the memtable out as a level-0 table now, and compacts if needed.
    void flush();

    // Deletes everything and starts empty (used when a snapshot replaces the whole state).
    void clear();

    Stats stats() const;

    // The tables of every level, as the manifest describes them (for tests and diagnostics).
    std::vector<std::vector<TableInfo>> tables() const;

    const RecoveryInfo& recovery() const { return recovery_; }

private:
    struct Table {
        TableInfo info;
        std::shared_ptr<SstReader> reader;
    };
    using TablePtr = std::shared_ptr<Table>;

    // Startup.
    bool loadManifest();
    void writeManifest();
    void cleanDirectory();
    void replayWal();
    void openWal(std::uint64_t number);

    // Flushing and compaction.
    void flushMemtable();
    void maybeCompact();
    void compactLevel0();
    void compactLevel(int level);
    void runCompaction(int fromLevel, std::vector<TablePtr> from, std::vector<TablePtr> to, int outLevel);
    bool overlapsDeeperLevels(int outLevel, const std::string& smallest, const std::string& largest) const;
    std::uint64_t levelBytes(int level) const;
    std::uint64_t maxBytesForLevel(int level) const;
    std::vector<TablePtr> overlapping(int level, const std::string& smallest, const std::string& largest) const;
    TablePtr openTable(const TableInfo& info);

    std::vector<std::unique_ptr<Cursor>> cursorsNewestFirst() const;

    std::string tablePath(std::uint64_t number) const;
    std::string walPath(std::uint64_t number) const;
    std::string manifestPath() const;

    Options options_;
    MemTable memtable_;
    std::unique_ptr<storage::LogWriter> wal_;
    std::uint64_t walNumber_ = 0;
    std::uint64_t nextFile_ = 1;

    // levels_[0]: newest table first. levels_[n >= 1]: sorted by smallest key, never overlapping.
    std::vector<std::vector<TablePtr>> levels_;
    std::vector<std::string> compactPointer_;  // where the last compaction of each level ended (so they rotate)

    Stats stats_;
    RecoveryInfo recovery_;
};

}  // namespace kv::lsm
