#pragma once

#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace kv::lsm {

// What a lookup in one place (the memtable, a table) found for a key.
// A tombstone is a marker meaning "this key was deleted": it must be kept,
// because an older version of the key may still live in a deeper table, and
// the delete has to hide it.
struct LookupResult {
    bool tombstone = false;
    std::string value;  // empty for a tombstone
};

// The newest writes, held in memory in sorted order. Every write is also in
// the write-ahead log, so a crash loses nothing; when the memtable grows past
// a limit it is written out as an SSTable and emptied.
class MemTable {
public:
    struct Value {
        bool tombstone = false;
        std::string value;
    };
    using Map = std::map<std::string, Value, std::less<>>;  // sorted by key; std::less<> allows string_view lookups

    void put(std::string key, std::string value);
    void erase(std::string key);  // records a tombstone

    std::optional<LookupResult> get(std::string_view key) const;

    // A rough measure of the memory used, to decide when to flush.
    std::size_t approximateBytes() const { return bytes_; }
    std::size_t entries() const { return map_.size(); }
    bool empty() const { return map_.empty(); }
    void clear();

    const Map& map() const { return map_; }

private:
    void set(std::string key, Value value);

    Map map_;
    std::size_t bytes_ = 0;
};

}  // namespace kv::lsm
