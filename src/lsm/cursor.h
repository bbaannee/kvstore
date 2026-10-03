#pragma once

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "lsm/memtable.h"
#include "lsm/sstable.h"

namespace kv::lsm {

// A position in a sorted sequence of entries (a memtable, a table, or a merge
// of several). Reading a range of the database, and compaction, are both
// "walk the entries in key order", so they share this one interface.
class Cursor {
public:
    virtual ~Cursor() = default;

    virtual bool valid() const = 0;
    virtual const std::string& key() const = 0;
    virtual bool tombstone() const = 0;
    virtual const std::string& value() const = 0;

    virtual void seekToFirst() = 0;
    virtual void seek(std::string_view target) = 0;  // the first entry with key >= target
    virtual void next() = 0;
};

std::unique_ptr<Cursor> newMemTableCursor(const MemTable& table);
std::unique_ptr<Cursor> newTableCursor(std::shared_ptr<SstReader> table);

// Merges several cursors into one sorted sequence. When the same key appears
// in more than one source, only the entry from the EARLIEST source in `sources`
// is returned: callers list sources newest first, so the newest version of a
// key wins and the older ones are skipped. Tombstones are returned like any
// other entry (the caller decides whether to hide or keep them).
std::unique_ptr<Cursor> newMergingCursor(std::vector<std::unique_ptr<Cursor>> sources);

}  // namespace kv::lsm
