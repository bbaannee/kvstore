#include "lsm/memtable.h"

namespace kv::lsm {

namespace {

// Per-entry overhead of the map nodes and string headers, so that many tiny
// entries count for more than their payload bytes.
constexpr std::size_t kEntryOverhead = 64;

std::size_t sizeOf(const std::string& key, const MemTable::Value& v) { return key.size() + v.value.size() + kEntryOverhead; }

}  // namespace

void MemTable::set(std::string key, Value value) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        bytes_ -= sizeOf(it->first, it->second);
        it->second = std::move(value);
        bytes_ += sizeOf(it->first, it->second);
        return;
    }
    bytes_ += sizeOf(key, value);
    map_.emplace(std::move(key), std::move(value));
}

void MemTable::put(std::string key, std::string value) { set(std::move(key), Value{false, std::move(value)}); }

void MemTable::erase(std::string key) { set(std::move(key), Value{true, {}}); }

std::optional<LookupResult> MemTable::get(std::string_view key) const {
    auto it = map_.find(key);
    if (it == map_.end()) return std::nullopt;
    return LookupResult{it->second.tombstone, it->second.value};
}

void MemTable::clear() {
    map_.clear();
    bytes_ = 0;
}

}  // namespace kv::lsm
