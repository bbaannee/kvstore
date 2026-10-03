// Snapshots and log compaction in RaftStorage.

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "raft/raft_storage.h"
#include "storage/log_format.h"
#include "storage/log_reader.h"
#include "storage/log_record.h"
#include "storage/log_writer.h"
#include "temp_dir.h"

using namespace kv::raft;
using kv::storage::CorruptLogError;

namespace {

Entry entry(Term term, Index index, std::string data = "") {
    return Entry{term, index, Entry::Type::Command, std::move(data)};
}

StorageUpdate update(std::optional<HardState> hs, std::vector<Entry> entries = {}) {
    return StorageUpdate{hs, std::move(entries), std::nullopt};
}

Snapshot snapshotAt(Index index, Term term, std::string data = "state") {
    return Snapshot{index, term, Members{{1, "a:1"}, {2, "b:2"}, {3, "c:3"}}, std::move(data)};
}

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

// Builds a log file by hand from raw records, to create situations RaftStorage itself would never write.
class RawLog {
public:
    explicit RawLog(const std::string& path) : writer_(path, false) {}
    RawLog& update(const StorageUpdate& u) {
        writer_.appendPayload(encodeUpdate(u));
        return *this;
    }
    RawLog& snapshot(const Snapshot& s) {
        writer_.appendPayload(encodeSnapshot(s));
        return *this;
    }

private:
    kv::storage::LogWriter writer_;
};

}  // namespace

TEST(RaftStorageSnapshot, EncodingRoundTrips) {
    for (const Snapshot& s : {snapshotAt(5, 2), snapshotAt(1, 1, ""), snapshotAt(9, 3, std::string("a\0b", 3))}) {
        Snapshot out;
        ASSERT_TRUE(decodeSnapshot(encodeSnapshot(s), out));
        EXPECT_EQ(out, s);
    }
    const std::string full = encodeSnapshot(snapshotAt(5, 2));
    Snapshot out;
    for (std::size_t len = 0; len < full.size(); ++len)
        EXPECT_FALSE(decodeSnapshot(std::string_view(full).substr(0, len), out)) << len;
    EXPECT_FALSE(decodeSnapshot(full + "x", out));
}

TEST(RaftStorageSnapshot, ReplacesTheLogAndSurvivesARestart) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    std::size_t sizeBefore;
    {
        RaftStorage s(path);
        s.save(update(HardState{2, 1u}, {entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4), entry(2, 5)}));
        s.save(update(std::nullopt, {entry(2, 6, std::string(500, 'x'))}));
        sizeBefore = std::filesystem::file_size(path);

        // Snapshot at index 4: the log keeps only what comes after it.
        StorageUpdate u;
        u.snapshot = snapshotAt(4, 2);
        u.entries = {entry(2, 5), entry(2, 6, std::string(500, 'x'))};
        s.save(u);
    }
    // The file was replaced by a short one holding the snapshot and the 2 remaining entries.
    EXPECT_LT(std::filesystem::file_size(path), sizeBefore + 250);

    RaftStorage s(path);
    ASSERT_TRUE(s.recovery().state.snapshot);
    EXPECT_EQ(*s.recovery().state.snapshot, snapshotAt(4, 2));
    EXPECT_EQ(s.recovery().state.hardState, (HardState{2, 1u}));  // the hard state carried over into the new file
    ASSERT_EQ(s.recovery().state.log.size(), 2u);
    EXPECT_EQ(s.recovery().state.log[0].index, 5u);
    EXPECT_EQ(s.recovery().state.log[1].index, 6u);
}

TEST(RaftStorageSnapshot, CompactionReallyShrinksALongLog) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RaftStorage s(path, false);
    for (Index i = 1; i <= 200; ++i) s.save(update(std::nullopt, {entry(1, i, std::string(100, 'x'))}));
    const auto grown = std::filesystem::file_size(path);
    ASSERT_GT(grown, 20000u);

    StorageUpdate u;
    u.snapshot = snapshotAt(195, 1, "small state");
    for (Index i = 196; i <= 200; ++i) u.entries.push_back(entry(1, i, std::string(100, 'x')));
    s.save(u);
    EXPECT_LT(std::filesystem::file_size(path), 2000u);
}

TEST(RaftStorageSnapshot, SavingContinuesNormallyAfterACompaction) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{1, std::nullopt}, {entry(1, 1), entry(1, 2), entry(1, 3)}));
        StorageUpdate u;
        u.snapshot = snapshotAt(2, 1);
        u.entries = {entry(1, 3)};
        s.save(u);
        s.save(update(HardState{3, 2u}, {entry(3, 4), entry(3, 5)}));          // appends to the NEW file
        s.save(update(std::nullopt, {entry(3, 4, "replaced"), entry(3, 5)}));  // an overwrite after the snapshot
    }
    RaftStorage s(path);
    EXPECT_EQ(s.recovery().state.hardState, (HardState{3, 2u}));
    ASSERT_EQ(s.recovery().state.log.size(), 3u);
    EXPECT_EQ(s.recovery().state.log[1].data, "replaced");
}

// Compaction replaces the file with a rename, so a crash at any moment leaves
// either the whole old file or the whole new one.
TEST(RaftStorageSnapshot, ACompactionThatCrashedBeforeTheRenameChangesNothing) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{1, 1u}, {entry(1, 1), entry(1, 2)}));
    }
    writeFile(path + ".tmp", "a half-written new file");  // what the crash left behind

    RaftStorage s(path);
    EXPECT_FALSE(std::filesystem::exists(path + ".tmp"));
    EXPECT_FALSE(s.recovery().state.snapshot);
    EXPECT_EQ(s.recovery().state.log.size(), 2u);
}

// A snapshot record in the middle of a log: the same decision a node makes when
// it installs a snapshot from the leader.
TEST(RaftStorageSnapshot, ASnapshotThatAgreesWithTheLogKeepsTheEntriesAfterIt) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RawLog(path)
        .update(update(HardState{2, std::nullopt}, {entry(1, 1), entry(2, 2), entry(2, 3), entry(2, 4), entry(2, 5)}))
        .snapshot(snapshotAt(4, 2));  // entry 4 has term 2 here too

    RaftStorage s(path);
    EXPECT_EQ(s.recovery().state.snapshot->index, 4u);
    ASSERT_EQ(s.recovery().state.log.size(), 1u);
    EXPECT_EQ(s.recovery().state.log[0].index, 5u);
}

TEST(RaftStorageSnapshot, ASnapshotThatContradictsTheLogDiscardsIt) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RawLog(path)
        .update(update(HardState{2, std::nullopt}, {entry(1, 1), entry(2, 2), entry(2, 3), entry(2, 4), entry(2, 5)}))
        .snapshot(snapshotAt(4, 3))  // the cluster committed a different entry 4 (term 3)
        .update(update(HardState{3, std::nullopt}, {entry(3, 5, "from the new leader")}));

    RaftStorage s(path);
    EXPECT_EQ(s.recovery().state.snapshot->index, 4u);
    ASSERT_EQ(s.recovery().state.log.size(), 1u);  // not the old entry 5: the history it belonged to is gone
    EXPECT_EQ(s.recovery().state.log[0].data, "from the new leader");
}

TEST(RaftStorageSnapshot, ASnapshotBeyondTheWholeLogDiscardsIt) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RawLog(path).update(update(HardState{1, std::nullopt}, {entry(1, 1), entry(1, 2)})).snapshot(snapshotAt(10, 1));

    RaftStorage s(path);
    EXPECT_EQ(s.recovery().state.snapshot->index, 10u);
    EXPECT_TRUE(s.recovery().state.log.empty());  // nothing wrong with that: it is NOT a gap

    s.save(update(std::nullopt, {entry(1, 11)}));  // the next entry continues right after the snapshot
    EXPECT_EQ(RaftStorage(path).recovery().state.log.size(), 1u);
}

TEST(RaftStorageSnapshot, RulesAroundTheSnapshotAreEnforced) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");

    RawLog(path).snapshot(snapshotAt(10, 1)).update(update(std::nullopt, {entry(1, 12)}));  // 11 is missing
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);

    std::filesystem::remove(path);
    RawLog(path).snapshot(snapshotAt(10, 1)).update(update(std::nullopt, {entry(1, 10)}));  // an entry the snapshot covers
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);

    std::filesystem::remove(path);
    RawLog(path).snapshot(snapshotAt(10, 1)).snapshot(snapshotAt(7, 1));  // snapshots never go backwards
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);

    std::filesystem::remove(path);
    RawLog(path).snapshot(snapshotAt(10, 3)).update(update(std::nullopt, {entry(2, 11)}));  // entry older than the snapshot's term
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);
}

TEST(RaftStorageSnapshot, ADamagedSnapshotRecordIsCorruptionNotATornTail) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RawLog(path).snapshot(snapshotAt(5, 2, std::string(300, 'x'))).update(update(HardState{2, 1u}, {entry(2, 6)}));
    std::string bytes = readFile(path);
    bytes[100] = static_cast<char>(bytes[100] ^ 0x01);  // inside the snapshot record, which is followed by another record
    writeFile(path, bytes);
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);
}

TEST(RaftStorageSnapshot, RefusesToSaveASnapshotThatCouldNotBeReadBack) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RaftStorage s(path, false);
    s.save(update(HardState{1, 1u}, {entry(1, 1)}));

    StorageUpdate u;
    u.snapshot = snapshotAt(1, 1, std::string(kv::storage::kMaxPayloadSize + 1, 'x'));
    EXPECT_THROW(s.save(u), std::runtime_error);

    // The old file is untouched and still works.
    RaftStorage again(path, false);
    EXPECT_EQ(again.recovery().state.log.size(), 1u);
}
