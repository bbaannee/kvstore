#include "lsm/cursor.h"

namespace kv::lsm {

namespace {

class MemTableCursor : public Cursor {
public:
    explicit MemTableCursor(const MemTable& table) : map_(table.map()), it_(map_.end()) {}

    bool valid() const override { return it_ != map_.end(); }
    const std::string& key() const override { return it_->first; }
    bool tombstone() const override { return it_->second.tombstone; }
    const std::string& value() const override { return it_->second.value; }

    void seekToFirst() override { it_ = map_.begin(); }
    void seek(std::string_view target) override { it_ = map_.lower_bound(target); }
    void next() override { ++it_; }

private:
    const MemTable::Map& map_;
    MemTable::Map::const_iterator it_;
};

class TableCursor : public Cursor {
public:
    explicit TableCursor(std::shared_ptr<SstReader> table) : table_(std::move(table)), it_(table_.get()) {}

    bool valid() const override { return it_.valid(); }
    const std::string& key() const override { return it_.key(); }
    bool tombstone() const override { return it_.tombstone(); }
    const std::string& value() const override { return it_.value(); }

    void seekToFirst() override { it_.seekToFirst(); }
    void seek(std::string_view target) override { it_.seek(target); }
    void next() override { it_.next(); }

private:
    std::shared_ptr<SstReader> table_;  // keeps the table alive while we read it
    SstReader::Iterator it_;
};

class MergingCursor : public Cursor {
public:
    explicit MergingCursor(std::vector<std::unique_ptr<Cursor>> sources) : sources_(std::move(sources)) {}

    bool valid() const override { return current_ != kNone; }
    const std::string& key() const override { return sources_[current_]->key(); }
    bool tombstone() const override { return sources_[current_]->tombstone(); }
    const std::string& value() const override { return sources_[current_]->value(); }

    void seekToFirst() override {
        for (auto& s : sources_) s->seekToFirst();
        pickSmallest();
    }

    void seek(std::string_view target) override {
        for (auto& s : sources_) s->seek(target);
        pickSmallest();
    }

    void next() override {
        // Step past this key in EVERY source that has it, so older versions of it are skipped.
        const std::string current = key();
        for (auto& s : sources_)
            if (s->valid() && s->key() == current) s->next();
        pickSmallest();
    }

private:
    static constexpr std::size_t kNone = ~std::size_t{0};

    // The current entry is the smallest key; among equal keys, the earliest source (the newest).
    void pickSmallest() {
        current_ = kNone;
        for (std::size_t i = 0; i < sources_.size(); ++i) {
            if (!sources_[i]->valid()) continue;
            if (current_ == kNone || sources_[i]->key() < sources_[current_]->key()) current_ = i;
        }
    }

    std::vector<std::unique_ptr<Cursor>> sources_;
    std::size_t current_ = kNone;
};

}  // namespace

std::unique_ptr<Cursor> newMemTableCursor(const MemTable& table) { return std::make_unique<MemTableCursor>(table); }

std::unique_ptr<Cursor> newTableCursor(std::shared_ptr<SstReader> table) {
    return std::make_unique<TableCursor>(std::move(table));
}

std::unique_ptr<Cursor> newMergingCursor(std::vector<std::unique_ptr<Cursor>> sources) {
    return std::make_unique<MergingCursor>(std::move(sources));
}

}  // namespace kv::lsm
