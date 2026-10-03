#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "lsm/memtable.h"
#include "storage/file_util.h"

namespace kv::lsm {

// Thrown when a table file is damaged. Unlike the end of a write-ahead log,
// a table is written once, completely, and only then becomes part of the
// database, so there is no "half-written but harmless" case: any damage is
// real corruption.
class CorruptTableError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// What the database needs to know about a table without opening it.
struct TableInfo {
    std::uint64_t fileNumber = 0;
    std::uint64_t fileSize = 0;
    std::uint64_t entries = 0;
    std::string smallest;  // the smallest and largest key in the table
    std::string largest;
};

// An SSTable ("sorted string table"): an immutable file of key-value entries
// sorted by key. This is what a memtable becomes when it is flushed, and what
// compaction produces.
//
//   ┌────────────┬────────────┬─────┬──────────────┬─────────────┬────────┐
//   │ data block │ data block │ ... │ filter block │ index block │ footer │
//   └────────────┴────────────┴─────┴──────────────┴─────────────┴────────┘
//
//   data block   a run of entries (about 4 KB), then a CRC-32C of the entries:
//                  entry = [flag: u8, 1 = value, 0 = tombstone][key_len: u32][key][value_len: u32][value]
//   filter block a Bloom filter of every key in the table, then its CRC-32C
//   index block  one line per data block, to find the right block for a key without
//                reading the others, then its CRC-32C:
//                  [block_count: u32][entry_count: u64][smallest: u32 len + bytes][largest: u32 len + bytes]
//                  per block: [last_key: u32 len + bytes][offset: u64][size: u32]
//   footer       fixed size, always at the very end of the file, so a reader finds
//                everything else from it:
//                  [filter_offset: u64][filter_size: u64][index_offset: u64][index_size: u64]
//                  [entry_count: u64][magic: u64][crc32c of the 48 bytes before: u32]
//
// A lookup: ask the Bloom filter (a "no" ends it), binary-search the index for
// the one block that could hold the key, read just that block, search it.
// (The size in an index line is the whole block, including its trailing CRC.)

inline constexpr std::uint64_t kSstMagic = 0x4B56535354414231ull;  // "KVSSTAB1"
inline constexpr std::size_t kFooterSize = 8 * 6 + 4;

// Writes a table. Entries must be added in strictly increasing key order.
class SstWriter {
public:
    // `path` is created (an existing file is replaced).
    SstWriter(const std::string& path, std::size_t blockBytes, int bloomBitsPerKey, bool sync);
    ~SstWriter();
    SstWriter(const SstWriter&) = delete;
    SstWriter& operator=(const SstWriter&) = delete;

    void add(std::string_view key, bool tombstone, std::string_view value);

    // The size of the file so far (so a compaction can cut tables at a target size).
    std::uint64_t bytesWritten() const { return fileBytes_ + buffer_.size() + block_.size(); }
    std::uint64_t entries() const { return entries_; }

    // Writes the filter, index and footer, makes the file durable, and closes it.
    // The table must not be empty.
    TableInfo finish();

private:
    void flushBlock();
    void append(std::string_view bytes);
    void flushBuffer();

    std::string path_;
    storage::UniqueFd fd_;
    std::size_t blockBytes_;
    int bloomBitsPerKey_;
    bool sync_;

    std::string buffer_;     // bytes waiting to be written to the file
    std::string block_;      // the data block being built
    std::string lastKey_;    // the last key added
    std::string firstKey_;
    std::vector<std::string> keys_;  // for the Bloom filter

    struct IndexLine {
        std::string lastKey;
        std::uint64_t offset;
        std::uint32_t size;
    };
    std::vector<IndexLine> index_;

    std::uint64_t fileBytes_ = 0;  // bytes already handed to write()
    std::uint64_t entries_ = 0;
    bool finished_ = false;
};

// Reads a table. Opening reads the footer, the index and the filter into
// memory; data blocks are read from the file one at a time, when needed.
class SstReader {
public:
    // Throws CorruptTableError if the file isn't a valid table.
    static std::shared_ptr<SstReader> open(const std::string& path);

    std::optional<LookupResult> get(std::string_view key) const;

    const std::string& smallest() const { return smallest_; }
    const std::string& largest() const { return largest_; }
    std::uint64_t entries() const { return entries_; }
    std::uint64_t fileSize() const { return fileSize_; }

    // Walks the entries in key order.
    class Iterator {
    public:
        explicit Iterator(const SstReader* table) : table_(table) {}

        bool valid() const { return valid_; }
        const std::string& key() const { return items_[pos_].key; }
        bool tombstone() const { return items_[pos_].tombstone; }
        const std::string& value() const { return items_[pos_].value; }

        void seekToFirst();
        void seek(std::string_view target);  // the first entry with key >= target
        void next();

    private:
        void loadBlock(std::size_t block);

        const SstReader* table_;
        bool valid_ = false;
        std::size_t block_ = 0;
        std::size_t pos_ = 0;
        struct Item {
            std::string key;
            bool tombstone;
            std::string value;
        };
        std::vector<Item> items_;
        friend class SstReader;
    };

private:
    SstReader() = default;

    struct IndexLine {
        std::string lastKey;
        std::uint64_t offset;
        std::uint32_t size;
    };

    // Reads and verifies one data block, returning its entries.
    void readBlock(std::size_t block, std::vector<Iterator::Item>& out) const;
    std::string readAt(std::uint64_t offset, std::uint64_t size) const;
    std::size_t blockFor(std::string_view key) const;  // the first block whose last key is >= key (or index_.size())

    std::string path_;
    storage::UniqueFd fd_;
    std::uint64_t fileSize_ = 0;
    std::uint64_t entries_ = 0;
    std::string smallest_, largest_;
    std::string filter_;
    std::vector<IndexLine> index_;
};

}  // namespace kv::lsm
