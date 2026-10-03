#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include "lsm/lsm_tree.h"

namespace kv::server {

// Where the state machine keeps its data. Two implementations:
//   MemoryStorage  a hash map: fast, bounded by RAM, gone after a restart
//                  (the state is rebuilt from the Raft snapshot and log)
//   LsmStorage     an LSM tree on disk: bigger than RAM, and it survives a restart
//                  by itself, so a restarted server doesn't replay its whole history
//
// Besides the user's keys, a storage holds the state machine's BOOKKEEPING,
// which must always agree with the data: the index of the last Raft log entry
// that was applied, and the table of client sessions. After a restart, the
// applied index tells Raft which entries still need to be applied. So the
// bookkeeping changes in the same atomic batch as the data it describes: after
// a crash, the data and the applied index are always from the same point.
class KvStorage {
public:
    virtual ~KvStorage() = default;

    // User data.
    virtual std::optional<std::string> get(const std::string& key) const = 0;
    virtual void put(const std::string& key, const std::string& value) = 0;
    virtual bool erase(const std::string& key) = 0;  // returns whether the key existed
    virtual void forEach(const std::function<void(const std::string&, const std::string&)>& fn) const = 0;
    virtual std::size_t size() const = 0;  // the number of keys (may be slow: a scan)

    // Bookkeeping.
    virtual std::uint64_t appliedIndex() const = 0;
    virtual void setAppliedIndex(std::uint64_t index) = 0;
    virtual std::optional<std::string> getMeta(const std::string& key) const = 0;
    virtual void putMeta(const std::string& key, const std::string& value) = 0;
    virtual void eraseMeta(const std::string& key) = 0;
    virtual void forEachMeta(const std::string& prefix,
                             const std::function<void(const std::string&, const std::string&)>& fn) const = 0;

    // Everything written between beginBatch() and commitBatch() becomes durable
    // together, or not at all. Reads in between see the pending writes.
    virtual void beginBatch() = 0;
    virtual void commitBatch() = 0;

    virtual void clear() = 0;  // everything: data and bookkeeping (the applied index becomes 0)
    virtual void sync() = 0;   // make everything written so far durable now
};

class MemoryStorage : public KvStorage {
public:
    std::optional<std::string> get(const std::string& key) const override;
    void put(const std::string& key, const std::string& value) override { data_[key] = value; }
    bool erase(const std::string& key) override { return data_.erase(key) > 0; }
    void forEach(const std::function<void(const std::string&, const std::string&)>& fn) const override;
    std::size_t size() const override { return data_.size(); }

    std::uint64_t appliedIndex() const override { return applied_; }
    void setAppliedIndex(std::uint64_t index) override { applied_ = index; }
    std::optional<std::string> getMeta(const std::string& key) const override;
    void putMeta(const std::string& key, const std::string& value) override { meta_[key] = value; }
    void eraseMeta(const std::string& key) override { meta_.erase(key); }
    void forEachMeta(const std::string& prefix,
                     const std::function<void(const std::string&, const std::string&)>& fn) const override;

    void beginBatch() override {}   // nothing is durable anyway
    void commitBatch() override {}
    void clear() override;
    void sync() override {}

private:
    std::unordered_map<std::string, std::string> data_;
    std::map<std::string, std::string> meta_;
    std::uint64_t applied_ = 0;
};

// The LSM tree holds both kinds of data in one key space, told apart by a
// one-byte prefix: user key "k" is stored as "uk", bookkeeping key "k" as "mk".
class LsmStorage : public KvStorage {
public:
    // Opens (or creates) the database in options.dir.
    explicit LsmStorage(lsm::Options options);

    std::optional<std::string> get(const std::string& key) const override;
    void put(const std::string& key, const std::string& value) override;
    bool erase(const std::string& key) override;
    void forEach(const std::function<void(const std::string&, const std::string&)>& fn) const override;
    std::size_t size() const override;

    std::uint64_t appliedIndex() const override;
    void setAppliedIndex(std::uint64_t index) override;
    std::optional<std::string> getMeta(const std::string& key) const override;
    void putMeta(const std::string& key, const std::string& value) override;
    void eraseMeta(const std::string& key) override;
    void forEachMeta(const std::string& prefix,
                     const std::function<void(const std::string&, const std::string&)>& fn) const override;

    void beginBatch() override;
    void commitBatch() override;
    void clear() override;
    void sync() override { tree_->sync(); }

    const lsm::LsmTree& tree() const { return *tree_; }

private:
    std::optional<std::string> read(const std::string& fullKey) const;
    void stagePut(std::string fullKey, std::string value);
    void stageErase(std::string fullKey);

    std::unique_ptr<lsm::LsmTree> tree_;

    // Writes of the batch in progress. They are not in the tree yet, but reads must see them.
    bool inBatch_ = false;
    lsm::WriteBatch batch_;
    std::map<std::string, std::optional<std::string>> pending_;
};

}  // namespace kv::server
