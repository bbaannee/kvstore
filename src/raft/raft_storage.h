#pragma once

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "raft/types.h"
#include "storage/log_writer.h"

namespace kv::raft {

// Everything a Raft node must remember across a crash.
struct RaftState {
    HardState hardState;
    std::vector<Entry> log;  // the entries AFTER the snapshot: log[i] has index snapshot.index + 1 + i
    std::optional<Snapshot> snapshot;
};

// What one Ready asks us to persist. The term/vote and the entries are saved
// as a single record, so they are all-or-nothing: after a crash we see either
// the whole update or none of it, never "the vote but not the entries".
struct StorageUpdate {
    std::optional<HardState> hardState;
    std::vector<Entry> entries;  // replaces the log from entries.front().index on

    // If set: save this snapshot, and replace the whole stored log with
    // `entries` (which then holds every entry after the snapshot).
    std::optional<Snapshot> snapshot;

    bool empty() const { return !hardState && !snapshot && entries.empty(); }
    bool operator==(const StorageUpdate&) const = default;
};

// The file is a sequence of checksummed records (see storage/log_format.h).
// Two kinds of record payload, told apart by their first byte (integers little-endian):
//
//   Update:
//     [tag: u8 = 0x52]
//     [flags: u8]                      bit 0: a HardState follows
//     [term: u64][has_vote: u8][voted_for: u64]     (only if bit 0 is set)
//     [entry_count: u32]
//     entry_count times: an entry, see raft/entry_codec.h
//
//   Snapshot:
//     [tag: u8 = 0x53][index: u64][term: u64][members][data_len: u32][data]
std::string encodeUpdate(const StorageUpdate& update);

// Returns false if the payload is malformed (never reads out of bounds).
bool decodeUpdate(std::string_view payload, StorageUpdate& out);

std::string encodeSnapshot(const Snapshot& snapshot);
bool decodeSnapshot(std::string_view payload, Snapshot& out);

// Durable storage for a Raft node: term, vote, log and snapshot, all in ONE
// checksummed write-ahead log file.
//
// Every save() appends one Update record and fsyncs. On open, the records are
// replayed in order to rebuild the state:
//   - an Update's HardState replaces the term/vote;
//   - an Update's entries overwrite the log from their first index onward
//     (this is how a follower's conflicting entries get replaced);
//   - a Snapshot record installs the snapshot: the log up to its index is
//     dropped (if our log agrees with it at that index, what follows is kept;
//     if not, the whole log is obsolete and is discarded).
//
// Compaction. Saving a snapshot replaces the whole file with a short new one:
//   1. write `<path>.tmp` containing [Snapshot][Update with the current hard
//      state and the entries after the snapshot], and fsync it;
//   2. rename it over `<path>` (atomic), and fsync the directory.
// The snapshot and the log can never disagree after a crash, because they are
// in the same file and the rename replaces both at once. (An earlier design
// kept the snapshot in a second file; after a crash between the two updates,
// a snapshot and an older log that contradicted it could coexist, and
// recovery had to guess which one was right.)
class RaftStorage {
public:
    struct Recovery {
        RaftState state;
        std::size_t updatesReplayed = 0;
        bool truncatedTornTail = false;  // a half-written record was cut off
    };

    // Opens (or creates) the storage and recovers the state. Throws
    // CorruptLogError if the file is damaged anywhere but its end, or if its
    // contents break Raft's rules (a gap in the log, a term that goes
    // backwards, ...). syncOnAppend=false is for tests only.
    explicit RaftStorage(const std::string& path, bool syncOnAppend = true);

    const Recovery& recovery() const { return recovery_; }

    // Persists an update; returns only once it is safely on disk.
    void save(const StorageUpdate& update);

private:
    static Recovery recover(const std::string& path);
    void compact(const Snapshot& snapshot, const std::vector<Entry>& entries);

    std::string path_;
    bool sync_;
    Recovery recovery_;    // computed before the writer opens the file for appending
    HardState hardState_;  // the newest hard state saved, needed to write a fresh file
    std::unique_ptr<storage::LogWriter> writer_;
};

}  // namespace kv::raft
