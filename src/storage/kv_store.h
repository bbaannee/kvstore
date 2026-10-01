#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>

#include "storage/log_reader.h"
#include "storage/log_record.h"
#include "storage/log_writer.h"

namespace kv::storage {

// What happened while opening the store.
struct RecoveryInfo {
    std::size_t recordsReplayed = 0;
    bool truncatedTornTail = false;  // a half-written record was cut off
    std::uint64_t validBytes = 0;    // size of the log after recovery
};

// A durable key-value store backed by a write-ahead log.
//
// Every change is first appended to the log (and fsync'ed), and only then
// applied to the in-memory map. On open, the log is replayed to rebuild the
// map, so every change that returned successfully survives a crash.
class KvStore {
public:
    // Opens (or creates) the store whose log lives at `logPath`.
    // Throws CorruptLogError if the log is damaged anywhere but its end.
    explicit KvStore(const std::string& logPath);

    void put(const std::string& key, const std::string& value);

    // Returns false if the key didn't exist (nothing is logged then).
    bool del(const std::string& key);

    std::optional<std::string> get(const std::string& key) const;

    std::size_t size() const { return data_.size(); }
    const RecoveryInfo& recovery() const { return recovery_; }

private:
    RecoveryInfo replay(const std::string& logPath);

    // Changes the map only; never touches the log. Used both for new
    // changes and for replaying old ones.
    void apply(const LogRecord& rec);

    // Declaration order matters: the map is filled by replay() before the
    // writer opens the log for new appends.
    std::unordered_map<std::string, std::string> data_;
    RecoveryInfo recovery_;
    LogWriter log_;
};

}  // namespace kv::storage
