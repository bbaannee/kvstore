#include "lsm/lsm_tree.h"

#include <algorithm>
#include <cstdio>
#include <filesystem>
#include <set>

#include "common/coding.h"
#include "common/crc32c.h"
#include "lsm/cursor.h"
#include "storage/file_util.h"
#include "storage/log_reader.h"

namespace kv::lsm {

namespace fs = std::filesystem;

namespace {

constexpr std::uint64_t kManifestMagic = 0x4B564C534D4D4631ull;  // "KVLSMMF1"
constexpr std::uint8_t kBatchTag = 0x42;                          // 'B'
constexpr std::uint8_t kPutOp = 1;
constexpr std::uint8_t kEraseOp = 2;
constexpr std::size_t kMaxItem = 256 * 1024 * 1024;

void putBytes(std::string& out, std::string_view s) {
    putFixed32(out, static_cast<std::uint32_t>(s.size()));
    out.append(s);
}

// A batch as one write-ahead-log record. One record means all-or-nothing
// after a crash (the log's checksums make a half-written record detectable).
std::string encodeBatch(const WriteBatch& batch) {
    std::string out;
    out += static_cast<char>(kBatchTag);
    putFixed32(out, static_cast<std::uint32_t>(batch.size()));
    for (const WriteBatch::Op& op : batch.ops()) {
        out += static_cast<char>(op.erase ? kEraseOp : kPutOp);
        putBytes(out, op.key);
        if (!op.erase) putBytes(out, op.value);
    }
    return out;
}

bool decodeBatch(std::string_view payload, WriteBatch& batch) {
    ByteReader in(payload);
    std::uint8_t tag;
    std::uint32_t count;
    if (!in.getU8(tag) || tag != kBatchTag || !in.getFixed32(count) || count > in.remaining() / 5) return false;
    for (std::uint32_t i = 0; i < count; ++i) {
        std::uint8_t type;
        std::string key, value;
        if (!in.getU8(type) || !in.getLengthPrefixed(kMaxItem, key)) return false;
        if (type == kPutOp) {
            if (!in.getLengthPrefixed(kMaxItem, value)) return false;
            batch.put(std::move(key), std::move(value));
        } else if (type == kEraseOp) {
            batch.erase(std::move(key));
        } else {
            return false;
        }
    }
    return in.atEnd();
}

std::string numbered(const std::string& dir, std::uint64_t number, const char* prefix, const char* suffix) {
    char name[64];
    std::snprintf(name, sizeof(name), "%s%06llu%s", prefix, static_cast<unsigned long long>(number), suffix);
    return (fs::path(dir) / name).string();
}

}  // namespace

std::string LsmTree::tablePath(std::uint64_t number) const { return numbered(options_.dir, number, "", ".sst"); }
std::string LsmTree::walPath(std::uint64_t number) const { return numbered(options_.dir, number, "wal-", ".log"); }
std::string LsmTree::manifestPath() const { return (fs::path(options_.dir) / "MANIFEST").string(); }

// ---------------------------------------------------------------------------
// Opening and recovery
// ---------------------------------------------------------------------------

LsmTree::LsmTree(Options options) : options_(std::move(options)) {
    fs::create_directories(options_.dir);
    levels_.resize(static_cast<std::size_t>(options_.maxLevels));
    compactPointer_.resize(levels_.size());
    stats_.tablesPerLevel.assign(levels_.size(), 0);
    stats_.bytesPerLevel.assign(levels_.size(), 0);

    if (!loadManifest()) {
        // A brand new database. Its first manifest must exist before the first
        // write is acknowledged, so that the log we are about to write is the
        // one recovery will look for.
        walNumber_ = nextFile_++;
        writeManifest();
    }
    cleanDirectory();
    replayWal();
    openWal(walNumber_);
}

LsmTree::~LsmTree() = default;

LsmTree::TablePtr LsmTree::openTable(const TableInfo& info) {
    auto table = std::make_shared<Table>();
    table->info = info;
    table->reader = SstReader::open(tablePath(info.fileNumber));
    if (table->reader->smallest() != info.smallest || table->reader->largest() != info.largest ||
        table->reader->entries() != info.entries)
        throw CorruptTableError(tablePath(info.fileNumber) + " does not match the manifest");
    return table;
}

// The manifest is replaced atomically, so it is never half-written: damage
// to it is corruption, not a crash artifact.
bool LsmTree::loadManifest() {
    std::string bytes;
    {
        if (!fs::exists(manifestPath())) return false;
        std::FILE* f = std::fopen(manifestPath().c_str(), "rb");
        if (!f) storage::throwErrno("open " + manifestPath());
        char buf[4096];
        std::size_t n;
        while ((n = std::fread(buf, 1, sizeof(buf), f)) > 0) bytes.append(buf, n);
        std::fclose(f);
    }

    auto corrupt = [&](const std::string& what) { throw CorruptTableError(manifestPath() + ": " + what); };
    if (bytes.size() < 4 || decodeFixed32(bytes.data() + bytes.size() - 4) != crc32c(std::string_view(bytes).substr(0, bytes.size() - 4)))
        corrupt("bad checksum");

    ByteReader in(std::string_view(bytes).substr(0, bytes.size() - 4));
    std::uint64_t magic, tables;
    std::uint32_t count;
    if (!in.getFixed64(magic) || magic != kManifestMagic || !in.getFixed64(nextFile_) || !in.getFixed64(walNumber_) ||
        !in.getFixed32(count))
        corrupt("malformed header");
    tables = count;
    for (std::uint64_t i = 0; i < tables; ++i) {
        std::uint32_t level;
        TableInfo info;
        if (!in.getFixed32(level) || level >= levels_.size() || !in.getFixed64(info.fileNumber) ||
            !in.getFixed64(info.fileSize) || !in.getFixed64(info.entries) || !in.getLengthPrefixed(kMaxItem, info.smallest) ||
            !in.getLengthPrefixed(kMaxItem, info.largest))
            corrupt("malformed table entry");
        levels_[level].push_back(openTable(info));
    }
    if (!in.atEnd()) corrupt("trailing bytes");

    // The file lists level 0 oldest first (flush order); we keep it newest first.
    std::reverse(levels_[0].begin(), levels_[0].end());
    for (std::size_t l = 1; l < levels_.size(); ++l)
        std::sort(levels_[l].begin(), levels_[l].end(), [](const TablePtr& a, const TablePtr& b) { return a->info.smallest < b->info.smallest; });
    recovery_.tablesLoaded = static_cast<std::size_t>(tables);
    return true;
}

void LsmTree::writeManifest() {
    std::string out;
    putFixed64(out, kManifestMagic);
    putFixed64(out, nextFile_);
    putFixed64(out, walNumber_);

    std::uint32_t count = 0;
    for (const auto& level : levels_) count += static_cast<std::uint32_t>(level.size());
    putFixed32(out, count);
    for (std::size_t l = 0; l < levels_.size(); ++l) {
        // Level 0 is written oldest first, so that reading it back and reversing restores newest-first.
        std::vector<TablePtr> tables = levels_[l];
        if (l == 0) std::reverse(tables.begin(), tables.end());
        for (const TablePtr& t : tables) {
            putFixed32(out, static_cast<std::uint32_t>(l));
            putFixed64(out, t->info.fileNumber);
            putFixed64(out, t->info.fileSize);
            putFixed64(out, t->info.entries);
            putBytes(out, t->info.smallest);
            putBytes(out, t->info.largest);
        }
    }
    putFixed32(out, crc32c(out));

    // Write a new file next to the manifest and swap it in with one atomic rename: after a crash
    // the manifest is either the whole old one or the whole new one.
    const std::string temp = manifestPath() + ".tmp";
    {
        std::FILE* f = std::fopen(temp.c_str(), "wb");
        if (!f) storage::throwErrno("open " + temp);
        if (std::fwrite(out.data(), 1, out.size(), f) != out.size()) {
            std::fclose(f);
            storage::throwErrno("write " + temp);
        }
        std::fflush(f);
        if (options_.syncTables) ::fsync(::fileno(f));
        std::fclose(f);
    }
    fs::rename(temp, manifestPath());
    if (options_.syncTables) storage::fsyncParentDir(manifestPath());
}

// Deletes what isn't part of the database: tables written by a flush or
// compaction that crashed before the manifest was updated, logs older than
// the current one, temporary files.
void LsmTree::cleanDirectory() {
    std::set<std::string> keep;
    keep.insert(manifestPath());
    keep.insert(walPath(walNumber_));
    for (const auto& level : levels_)
        for (const TablePtr& t : level) keep.insert(tablePath(t->info.fileNumber));

    for (const auto& entry : fs::directory_iterator(options_.dir)) {
        if (!entry.is_regular_file() || keep.count(entry.path().string())) continue;
        fs::remove(entry.path());
    }
}

void LsmTree::replayWal() {
    storage::LogReader reader(walPath(walNumber_));  // a missing file reads as empty
    std::string payload;
    storage::ReadStatus status;
    while ((status = reader.nextPayload(payload)) == storage::ReadStatus::Ok) {
        WriteBatch batch;
        if (!decodeBatch(payload, batch))
            throw storage::CorruptLogError(walPath(walNumber_) + " is corrupted: malformed batch");
        for (const WriteBatch::Op& op : batch.ops()) {
            if (op.erase) memtable_.erase(op.key);
            else memtable_.put(op.key, op.value);
        }
        ++recovery_.walBatchesReplayed;
    }
    if (status == storage::ReadStatus::Corrupt)
        throw storage::CorruptLogError(walPath(walNumber_) + " is corrupted: " + reader.error());
    if (status == storage::ReadStatus::Torn) {  // a crash mid-write: cut off the half-written record
        storage::truncateFile(walPath(walNumber_), reader.validBytes());
        recovery_.truncatedTornTail = true;
    }
}

void LsmTree::openWal(std::uint64_t number) {
    wal_ = std::make_unique<storage::LogWriter>(walPath(number), options_.syncWal);
}

// ---------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------

void LsmTree::put(std::string key, std::string value) {
    WriteBatch batch;
    batch.put(std::move(key), std::move(value));
    write(batch);
}

void LsmTree::erase(std::string key) {
    WriteBatch batch;
    batch.erase(std::move(key));
    write(batch);
}

void LsmTree::write(const WriteBatch& batch) {
    if (batch.empty()) return;

    // Log first, then memory: if we crash in between, replay redoes the write.
    wal_->appendPayload(encodeBatch(batch));
    for (const WriteBatch::Op& op : batch.ops()) {
        if (op.erase) memtable_.erase(op.key);
        else memtable_.put(op.key, op.value);
    }
    if (memtable_.approximateBytes() >= options_.memtableBytes) flushMemtable();
}

void LsmTree::sync() { wal_->sync(); }

void LsmTree::flush() { flushMemtable(); }

// memtable -> a new level-0 table. The order of the steps makes a crash at any point harmless:
//   1. start a new log, and write the table (neither is part of the database yet)
//   2. write a new manifest: this atomic step makes the table and the new log official
//   3. delete the old log
// Crash before 2: the old manifest still names the old log, which still holds everything,
//   and the new files are junk that recovery deletes.
// Crash after 2 and before 3: the manifest names the new log; the old one is junk.
void LsmTree::flushMemtable() {
    if (memtable_.empty()) return;

    const std::uint64_t newWal = nextFile_++;
    const std::uint64_t tableNumber = nextFile_++;

    SstWriter writer(tablePath(tableNumber), options_.blockBytes, options_.bloomBitsPerKey, options_.syncTables);
    for (const auto& [key, value] : memtable_.map()) writer.add(key, value.tombstone, value.value);
    TableInfo info = writer.finish();
    info.fileNumber = tableNumber;
    stats_.bytesWrittenByFlush += info.fileSize;

    auto oldWal = std::move(wal_);
    const std::uint64_t oldWalNumber = walNumber_;
    levels_[0].insert(levels_[0].begin(), openTable(info));
    walNumber_ = newWal;
    writeManifest();

    openWal(newWal);
    oldWal.reset();
    fs::remove(walPath(oldWalNumber));
    memtable_.clear();
    ++stats_.flushes;

    maybeCompact();
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

std::optional<std::string> LsmTree::get(std::string_view key) const {
    auto resolve = [](const LookupResult& r) -> std::optional<std::string> {
        if (r.tombstone) return std::nullopt;  // deleted: and an older version further down must not come back
        return r.value;
    };

    if (auto r = memtable_.get(key)) return resolve(*r);

    // Level 0: newest table first, because the tables may overlap and the newest version wins.
    for (const TablePtr& t : levels_[0])
        if (auto r = t->reader->get(key)) return resolve(*r);

    // Deeper levels don't overlap, so at most one table per level can hold the key.
    for (std::size_t l = 1; l < levels_.size(); ++l) {
        const auto& tables = levels_[l];
        auto it = std::lower_bound(tables.begin(), tables.end(), key,
                                   [](const TablePtr& t, std::string_view k) { return t->info.largest < k; });
        if (it == tables.end() || (*it)->info.smallest > key) continue;
        if (auto r = (*it)->reader->get(key)) return resolve(*r);
    }
    return std::nullopt;
}

// Every source of data, newest first: the memtable, then level 0 (newest table first), then each deeper level.
std::vector<std::unique_ptr<Cursor>> LsmTree::cursorsNewestFirst() const {
    std::vector<std::unique_ptr<Cursor>> cursors;
    cursors.push_back(newMemTableCursor(memtable_));
    for (const auto& level : levels_)
        for (const TablePtr& t : level) cursors.push_back(newTableCursor(t->reader));
    return cursors;
}

std::vector<std::pair<std::string, std::string>> LsmTree::scan(std::string_view start, std::string_view end,
                                                               std::size_t limit) const {
    std::vector<std::pair<std::string, std::string>> out;
    auto cursor = newMergingCursor(cursorsNewestFirst());
    cursor->seek(start);
    while (cursor->valid() && out.size() < limit && (end.empty() || cursor->key() < end)) {
        if (!cursor->tombstone()) out.emplace_back(cursor->key(), cursor->value());
        cursor->next();
    }
    return out;
}

void LsmTree::forEach(const std::function<void(const std::string&, const std::string&)>& fn) const {
    auto cursor = newMergingCursor(cursorsNewestFirst());
    for (cursor->seekToFirst(); cursor->valid(); cursor->next())
        if (!cursor->tombstone()) fn(cursor->key(), cursor->value());
}

void LsmTree::forEachInRange(std::string_view start, std::string_view end,
                             const std::function<void(const std::string&, const std::string&)>& fn) const {
    auto cursor = newMergingCursor(cursorsNewestFirst());
    for (cursor->seek(start); cursor->valid() && (end.empty() || cursor->key() < end); cursor->next())
        if (!cursor->tombstone()) fn(cursor->key(), cursor->value());
}

// ---------------------------------------------------------------------------
// Compaction
// ---------------------------------------------------------------------------

std::uint64_t LsmTree::levelBytes(int level) const {
    std::uint64_t total = 0;
    for (const TablePtr& t : levels_[static_cast<std::size_t>(level)]) total += t->info.fileSize;
    return total;
}

std::uint64_t LsmTree::maxBytesForLevel(int level) const {
    std::uint64_t bytes = options_.level1MaxBytes;
    for (int l = 1; l < level; ++l) bytes *= static_cast<std::uint64_t>(options_.levelMultiplier);
    return bytes;
}

std::vector<LsmTree::TablePtr> LsmTree::overlapping(int level, const std::string& smallest, const std::string& largest) const {
    std::vector<TablePtr> out;
    for (const TablePtr& t : levels_[static_cast<std::size_t>(level)])
        if (!(t->info.largest < smallest || t->info.smallest > largest)) out.push_back(t);
    return out;
}

bool LsmTree::overlapsDeeperLevels(int outLevel, const std::string& smallest, const std::string& largest) const {
    for (int l = outLevel + 1; l < static_cast<int>(levels_.size()); ++l)
        if (!overlapping(l, smallest, largest).empty()) return true;
    return false;
}

void LsmTree::maybeCompact() {
    while (true) {
        if (levels_[0].size() >= options_.l0CompactionTrigger) {
            compactLevel0();
            continue;
        }
        bool compacted = false;
        for (int level = 1; level + 1 < static_cast<int>(levels_.size()); ++level) {
            if (levelBytes(level) > maxBytesForLevel(level)) {
                compactLevel(level);
                compacted = true;
                break;
            }
        }
        if (!compacted) return;
    }
}

// All of level 0 (its tables overlap each other) plus the level-1 tables they overlap.
void LsmTree::compactLevel0() {
    std::string smallest = levels_[0].front()->info.smallest, largest = levels_[0].front()->info.largest;
    for (const TablePtr& t : levels_[0]) {
        smallest = std::min(smallest, t->info.smallest);
        largest = std::max(largest, t->info.largest);
    }
    runCompaction(0, levels_[0], overlapping(1, smallest, largest), 1);
}

// One table of an overfull level, plus the next level's tables it overlaps. The table is
// chosen round-robin by key (starting after where the last compaction of this level ended),
// so that every part of the key space gets compacted in turn.
void LsmTree::compactLevel(int level) {
    const auto& tables = levels_[static_cast<std::size_t>(level)];
    auto pointer = compactPointer_[static_cast<std::size_t>(level)];
    auto it = std::find_if(tables.begin(), tables.end(), [&](const TablePtr& t) { return t->info.smallest > pointer; });
    if (it == tables.end()) it = tables.begin();
    TablePtr victim = *it;
    compactPointer_[static_cast<std::size_t>(level)] = victim->info.largest;
    runCompaction(level, {victim}, overlapping(level + 1, victim->info.smallest, victim->info.largest), level + 1);
}

// Merges `from` (the newer data) and `to` (older) into new tables in outLevel.
// Where a key is in both, the version from `from` wins and the other disappears.
// A tombstone can be dropped for good if no deeper level could still hold an older
// version of the key (then there's nothing left for it to hide).
void LsmTree::runCompaction(int fromLevel, std::vector<TablePtr> from, std::vector<TablePtr> to, int outLevel) {
    std::string smallest = from.front()->info.smallest, largest = from.front()->info.largest;
    for (const auto* group : {&from, &to})
        for (const TablePtr& t : *group) {
            smallest = std::min(smallest, t->info.smallest);
            largest = std::max(largest, t->info.largest);
        }
    const bool dropTombstones = !overlapsDeeperLevels(outLevel, smallest, largest);

    // `from` is listed newest first (level 0 is kept that way), and all of it is newer than `to`.
    std::vector<std::unique_ptr<Cursor>> sources;
    for (const TablePtr& t : from) sources.push_back(newTableCursor(t->reader));
    for (const TablePtr& t : to) sources.push_back(newTableCursor(t->reader));
    auto merged = newMergingCursor(std::move(sources));

    std::vector<TablePtr> outputs;
    std::unique_ptr<SstWriter> writer;
    std::uint64_t writerNumber = 0;
    auto finishOutput = [&] {
        if (!writer) return;
        TableInfo info = writer->finish();
        info.fileNumber = writerNumber;
        stats_.bytesWrittenByCompaction += info.fileSize;
        outputs.push_back(openTable(info));
        writer.reset();
    };

    for (merged->seekToFirst(); merged->valid(); merged->next()) {
        if (merged->tombstone() && dropTombstones) continue;
        if (!writer) {
            writerNumber = nextFile_++;
            writer = std::make_unique<SstWriter>(tablePath(writerNumber), options_.blockBytes, options_.bloomBitsPerKey,
                                                 options_.syncTables);
        }
        writer->add(merged->key(), merged->tombstone(), merged->value());
        if (writer->bytesWritten() >= options_.targetFileBytes) finishOutput();
    }
    finishOutput();

    // Swap the inputs for the outputs, make that official with the manifest, and only then delete the old files.
    auto remove = [](std::vector<TablePtr>& level, const std::vector<TablePtr>& gone) {
        level.erase(std::remove_if(level.begin(), level.end(),
                                   [&](const TablePtr& t) { return std::find(gone.begin(), gone.end(), t) != gone.end(); }),
                    level.end());
    };
    remove(levels_[static_cast<std::size_t>(fromLevel)], from);
    remove(levels_[static_cast<std::size_t>(outLevel)], to);
    auto& out = levels_[static_cast<std::size_t>(outLevel)];
    out.insert(out.end(), outputs.begin(), outputs.end());
    std::sort(out.begin(), out.end(), [](const TablePtr& a, const TablePtr& b) { return a->info.smallest < b->info.smallest; });
    writeManifest();

    for (const auto* group : {&from, &to})
        for (const TablePtr& t : *group) fs::remove(tablePath(t->info.fileNumber));
    ++stats_.compactions;
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

void LsmTree::clear() {
    std::vector<std::uint64_t> obsolete;
    for (auto& level : levels_) {
        for (const TablePtr& t : level) obsolete.push_back(t->info.fileNumber);
        level.clear();
    }
    memtable_.clear();

    const std::uint64_t oldWal = walNumber_;
    wal_.reset();
    walNumber_ = nextFile_++;
    writeManifest();  // the database is empty from here on
    openWal(walNumber_);

    fs::remove(walPath(oldWal));
    for (std::uint64_t n : obsolete) fs::remove(tablePath(n));
}

std::vector<std::vector<TableInfo>> LsmTree::tables() const {
    std::vector<std::vector<TableInfo>> out(levels_.size());
    for (std::size_t l = 0; l < levels_.size(); ++l)
        for (const TablePtr& t : levels_[l]) out[l].push_back(t->info);
    return out;
}

Stats LsmTree::stats() const {
    Stats s = stats_;
    s.memtableBytes = memtable_.approximateBytes();
    for (std::size_t l = 0; l < levels_.size(); ++l) {
        s.tablesPerLevel[l] = levels_[l].size();
        s.bytesPerLevel[l] = levelBytes(static_cast<int>(l));
    }
    return s;
}

}  // namespace kv::lsm
