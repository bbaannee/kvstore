#pragma once

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "raft/types.h"
#include "server/command.h"
#include "server/kv_storage.h"

namespace kv::server {

// The answer to one applied command.
struct ApplyResult {
    bool found = false;  // Put: always true. Get/Del: did the key exist?
    std::string value;   // Get: the value, if found

    bool operator==(const ApplyResult&) const = default;
};

// The key-value map that Raft replicates. It's derived data: the result of
// applying the committed log, in order, from the beginning (or from a
// snapshot). The data lives in a KvStorage: in memory (rebuilt from the
// snapshot and the log after a restart) or in an LSM tree on disk (which
// remembers by itself how far it got: see appliedIndex()).
//
// apply() must be deterministic: every replica applies the same entries and
// must end up with the same map. So it depends on nothing but the entry
// (no clocks, no randomness) and never fails.
//
// Exactly-once execution. A client that gets no answer retries, so the same
// (clientId, seq) can appear in the log more than once. The state machine
// remembers each client's last seq and its result:
//   - seq newer than the last one: run it, remember the result;
//   - seq equal to the last one: a retry, return the remembered result;
//   - seq older: a stale duplicate, ignore it (the client already moved on).
// This table is part of the replicated state, so every replica agrees on
// which requests count as duplicates. It is bounded: when more than
// `maxSessions` clients are remembered, the one used longest ago is
// forgotten. "Longest ago" is measured in applied commands, not time, so
// all replicas forget the same client at the same point of the log. (A
// client that was forgotten and retries after that would be executed again,
// so the limit must be far above the number of clients that can have a
// request in flight at once.) The table is kept in memory and mirrored in
// the storage, so a persistent storage brings it back after a restart.
class KvStateMachine {
public:
    explicit KvStateMachine(std::size_t maxSessions = 10'000,
                            std::unique_ptr<KvStorage> storage = std::make_unique<MemoryStorage>());

    // Applies one committed entry. NoOp and Config entries (appended by
    // Raft itself) change no data, but still advance appliedIndex(). A command
    // that doesn't parse is ignored by everyone, identically, instead of
    // crashing some replicas and not others.
    ApplyResult apply(const raft::Entry& entry);

    // A read that changes nothing, for linearizable reads that Raft has
    // already confirmed (ReadIndex).
    ApplyResult read(const std::string& key) const;

    std::optional<std::string> get(const std::string& key) const { return storage_->get(key); }
    std::size_t size() const { return storage_->size(); }
    std::unordered_map<std::string, std::string> data() const;  // a copy of the whole map (for tests)
    std::size_t sessionCount() const { return sessions_.size(); }

    // The index of the last Raft log entry this state machine has applied. With
    // a persistent storage it survives a restart, and Raft then resumes from
    // here instead of replaying the log from the beginning.
    std::uint64_t appliedIndex() const { return storage_->appliedIndex(); }

    // Group several apply() calls into one atomic storage batch (one write-ahead
    // log record, instead of one per entry). Calls may be nested.
    void beginBatch();
    void endBatch();

    // Makes everything applied so far durable. Raft may discard log entries
    // only up to a point the state machine has synced.
    void sync() { storage_->sync(); }

    // The whole state (map and sessions) as bytes, for a Raft snapshot.
    // Deterministic: replicas with equal state produce equal bytes.
    //   [tag: u8 = 0x4B][use_counter: u64]
    //   [map_size: u64], per key (sorted): [key: u32 len + bytes][value: u32 len + bytes]
    //   [session_count: u64], per session (sorted by id):
    //       [client_id: u64][seq: u64][last_used: u64][found: u8][value: u32 len + bytes]
    std::string serialize() const;

    // Replaces the whole state with a serialized one, as of log index
    // `appliedIndex`. Returns false (and leaves the state empty) if the bytes
    // are malformed.
    bool restore(std::string_view bytes, std::uint64_t appliedIndex);

private:
    ApplyResult applyCommand(const raft::Entry& entry);
    ApplyResult execute(const storage::LogRecord& op);

    struct Session {
        std::uint64_t seq = 0;
        ApplyResult result;
        std::uint64_t lastUsed = 0;  // value of useCounter_ when this client last sent a request
    };

    void touch(std::uint64_t clientId, Session& session);  // marks the session as the most recently used, and saves it
    void loadSessions();                                    // after opening a storage that already has data
    void saveUseCounter();

    std::size_t maxSessions_;
    std::unique_ptr<KvStorage> storage_;
    std::unordered_map<std::uint64_t, Session> sessions_;  // by clientId
    std::map<std::uint64_t, std::uint64_t> byUse_;         // lastUsed -> clientId, oldest first
    std::uint64_t useCounter_ = 0;
    int batchDepth_ = 0;
};

}  // namespace kv::server
