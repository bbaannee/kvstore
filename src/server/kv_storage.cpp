#include "server/kv_storage.h"

#include "common/coding.h"

namespace kv::server {

namespace {

constexpr char kUserPrefix = 'u';
constexpr char kMetaPrefix = 'm';
const char* const kAppliedKey = "applied";

// The smallest string greater than every string that starts with `prefix`.
std::string prefixEnd(std::string prefix) {
    while (!prefix.empty()) {
        if (static_cast<unsigned char>(prefix.back()) != 0xFF) {
            prefix.back() = static_cast<char>(prefix.back() + 1);
            return prefix;
        }
        prefix.pop_back();
    }
    return {};  // no upper bound
}

}  // namespace

// ---------------------------------------------------------------------------
// MemoryStorage
// ---------------------------------------------------------------------------

std::optional<std::string> MemoryStorage::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

void MemoryStorage::forEach(const std::function<void(const std::string&, const std::string&)>& fn) const {
    for (const auto& [k, v] : data_) fn(k, v);
}

std::optional<std::string> MemoryStorage::getMeta(const std::string& key) const {
    auto it = meta_.find(key);
    if (it == meta_.end()) return std::nullopt;
    return it->second;
}

void MemoryStorage::forEachMeta(const std::string& prefix,
                                const std::function<void(const std::string&, const std::string&)>& fn) const {
    for (auto it = meta_.lower_bound(prefix); it != meta_.end() && it->first.compare(0, prefix.size(), prefix) == 0; ++it)
        fn(it->first, it->second);
}

void MemoryStorage::clear() {
    data_.clear();
    meta_.clear();
    applied_ = 0;
}

// ---------------------------------------------------------------------------
// LsmStorage
// ---------------------------------------------------------------------------

LsmStorage::LsmStorage(lsm::Options options) : tree_(std::make_unique<lsm::LsmTree>(std::move(options))) {}

std::optional<std::string> LsmStorage::read(const std::string& fullKey) const {
    if (inBatch_) {
        auto it = pending_.find(fullKey);
        if (it != pending_.end()) return it->second;  // a pending write (or a pending delete: nullopt)
    }
    return tree_->get(fullKey);
}

void LsmStorage::stagePut(std::string fullKey, std::string value) {
    if (inBatch_) {
        pending_[fullKey] = value;
        batch_.put(std::move(fullKey), std::move(value));
    } else {
        tree_->put(std::move(fullKey), std::move(value));
    }
}

void LsmStorage::stageErase(std::string fullKey) {
    if (inBatch_) {
        pending_[fullKey] = std::nullopt;
        batch_.erase(std::move(fullKey));
    } else {
        tree_->erase(std::move(fullKey));
    }
}

std::optional<std::string> LsmStorage::get(const std::string& key) const { return read(kUserPrefix + key); }

void LsmStorage::put(const std::string& key, const std::string& value) { stagePut(kUserPrefix + key, value); }

bool LsmStorage::erase(const std::string& key) {
    const std::string full = kUserPrefix + key;
    if (!read(full)) return false;  // nothing to delete: don't write a pointless tombstone
    stageErase(full);
    return true;
}

void LsmStorage::forEach(const std::function<void(const std::string&, const std::string&)>& fn) const {
    const std::string start(1, kUserPrefix);
    tree_->forEachInRange(start, prefixEnd(start), [&](const std::string& k, const std::string& v) { fn(k.substr(1), v); });
}

std::size_t LsmStorage::size() const {
    std::size_t n = 0;
    forEach([&](const std::string&, const std::string&) { ++n; });
    return n;
}

std::uint64_t LsmStorage::appliedIndex() const {
    auto bytes = getMeta(kAppliedKey);
    if (!bytes || bytes->size() != 8) return 0;
    return decodeFixed64(bytes->data());
}

void LsmStorage::setAppliedIndex(std::uint64_t index) {
    std::string bytes;
    putFixed64(bytes, index);
    putMeta(kAppliedKey, bytes);
}

std::optional<std::string> LsmStorage::getMeta(const std::string& key) const { return read(kMetaPrefix + key); }

void LsmStorage::putMeta(const std::string& key, const std::string& value) { stagePut(kMetaPrefix + key, value); }

void LsmStorage::eraseMeta(const std::string& key) { stageErase(kMetaPrefix + key); }

void LsmStorage::forEachMeta(const std::string& prefix,
                             const std::function<void(const std::string&, const std::string&)>& fn) const {
    const std::string start = kMetaPrefix + prefix;
    tree_->forEachInRange(start, prefixEnd(start), [&](const std::string& k, const std::string& v) { fn(k.substr(1), v); });
}

void LsmStorage::beginBatch() {
    inBatch_ = true;
    batch_.clear();
    pending_.clear();
}

void LsmStorage::commitBatch() {
    tree_->write(batch_);
    inBatch_ = false;
    batch_.clear();
    pending_.clear();
}

void LsmStorage::clear() {
    inBatch_ = false;
    batch_.clear();
    pending_.clear();
    tree_->clear();
}

}  // namespace kv::server
