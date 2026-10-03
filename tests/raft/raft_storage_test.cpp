#include "raft/raft_storage.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/log_reader.h"
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

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

// A deliberately simple model of what recovery should produce, used to
// check the real thing against.
RaftState model(const std::vector<StorageUpdate>& updates, std::size_t count) {
    RaftState s;
    for (std::size_t i = 0; i < count; ++i) {
        if (updates[i].hardState) s.hardState = *updates[i].hardState;
        if (!updates[i].entries.empty()) {
            s.log.resize(updates[i].entries.front().index - 1);
            s.log.insert(s.log.end(), updates[i].entries.begin(), updates[i].entries.end());
        }
    }
    return s;
}

}  // namespace

TEST(RaftStorage, StartsEmpty) {
    kv::test::TempDir dir;
    RaftStorage s(dir.file("raft.log"));
    EXPECT_EQ(s.recovery().updatesReplayed, 0u);
    EXPECT_EQ(s.recovery().state.hardState, HardState{});
    EXPECT_TRUE(s.recovery().state.log.empty());
}

TEST(RaftStorage, SavesAndRecovers) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{2, 3u}, {entry(1, 1, "a"), entry(2, 2, "b")}));
        s.save(update(std::nullopt, {entry(2, 3, "c")}));
        s.save(update(HardState{2, 3u}));  // unchanged hard state is fine
    }
    RaftStorage s(path);
    EXPECT_EQ(s.recovery().updatesReplayed, 3u);
    EXPECT_FALSE(s.recovery().truncatedTornTail);
    EXPECT_EQ(s.recovery().state.hardState, (HardState{2, 3u}));
    ASSERT_EQ(s.recovery().state.log.size(), 3u);
    EXPECT_EQ(s.recovery().state.log[2], entry(2, 3, "c"));
}

TEST(RaftStorage, EmptyUpdateWritesNothing) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    RaftStorage s(path);
    s.save(update(std::nullopt));
    EXPECT_EQ(std::filesystem::file_size(path), 0u);
}

// This is how a follower's conflicting entries get replaced.
TEST(RaftStorage, ConflictingEntriesOverwriteTheSuffix) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{1, std::nullopt}, {entry(1, 1), entry(1, 2), entry(1, 3)}));
        s.save(update(HardState{2, std::nullopt}, {entry(2, 2, "new")}));  // replaces 2 and 3
    }
    RaftStorage s(path);
    ASSERT_EQ(s.recovery().state.log.size(), 2u);
    EXPECT_EQ(s.recovery().state.log[0], entry(1, 1));
    EXPECT_EQ(s.recovery().state.log[1], entry(2, 2, "new"));
}

TEST(RaftStorage, VoteOfZeroIsNotNoVote) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{1, 0u}));
    }
    EXPECT_EQ(RaftStorage(path).recovery().state.hardState.votedFor, std::optional<NodeId>(0));
}

TEST(RaftStorage, UpdateEncodingRoundTrips) {
    for (const auto& u : std::vector<StorageUpdate>{
             update(std::nullopt),
             update(HardState{5, std::nullopt}),
             update(HardState{5, 7u}, {entry(5, 1)}),
             update(std::nullopt, {entry(1, 4, std::string("bin\0ary\n", 8)), Entry{2, 5, Entry::Type::NoOp, {}}}),
         }) {
        StorageUpdate out;
        ASSERT_TRUE(decodeUpdate(encodeUpdate(u), out));
        EXPECT_EQ(out, u);
    }
}

TEST(RaftStorage, DecodeRejectsEveryTruncationAndGarbage) {
    const std::string full = encodeUpdate(update(HardState{3, 2u}, {entry(3, 1, "hello"), entry(3, 2, "world")}));
    StorageUpdate out;
    for (std::size_t len = 0; len < full.size(); ++len)
        EXPECT_FALSE(decodeUpdate(full.substr(0, len), out)) << "length " << len;

    EXPECT_FALSE(decodeUpdate(full + "x", out));  // leftover bytes

    std::string bad = full;
    bad[0] = 'X';  // wrong tag
    EXPECT_FALSE(decodeUpdate(bad, out));
    bad = full;
    bad[1] = '\x02';  // unknown flag
    EXPECT_FALSE(decodeUpdate(bad, out));

    // An entry count far larger than the data could hold must be rejected
    // without trying to allocate for it.
    std::string huge = "\x52\x00";
    huge += std::string("\xFF\xFF\xFF\x7F", 4);
    EXPECT_FALSE(decodeUpdate(huge, out));
}

// Updates that pass their checksum but break Raft's rules mean a buggy
// writer. Replaying them would build a wrong state, so we refuse.
TEST(RaftStorage, RefusesUpdatesThatBreakRaftsRules) {
    struct Case {
        const char* name;
        std::vector<StorageUpdate> updates;
    };
    const std::vector<Case> cases = {
        {"gap in the log", {update(std::nullopt, {entry(1, 1)}), update(std::nullopt, {entry(1, 5)})}},
        {"log doesn't start at 1", {update(std::nullopt, {entry(1, 2)})}},
        {"non-contiguous entries", {update(std::nullopt, {entry(1, 1), entry(1, 3)})}},
        {"entry terms go backwards", {update(std::nullopt, {entry(2, 1), entry(1, 2)})}},
        {"term goes backwards", {update(HardState{5, std::nullopt}), update(HardState{4, std::nullopt})}},
        {"vote changes within a term", {update(HardState{5, 1u}), update(HardState{5, 2u})}},
    };
    for (const auto& c : cases) {
        SCOPED_TRACE(c.name);
        kv::test::TempDir dir;
        const auto path = dir.file("raft.log");
        {
            kv::storage::LogWriter w(path, false);  // bypass RaftStorage, which would be well-behaved
            for (const auto& u : c.updates) w.appendPayload(encodeUpdate(u));
        }
        EXPECT_THROW(RaftStorage{path}, CorruptLogError);
    }
}

TEST(RaftStorage, RefusesToOpenADamagedLog) {
    kv::test::TempDir dir;
    const auto path = dir.file("raft.log");
    {
        RaftStorage s(path);
        s.save(update(HardState{1, 1u}, {entry(1, 1, "first")}));
        s.save(update(std::nullopt, {entry(1, 2, "second")}));
    }
    std::string bytes = readFile(path);
    bytes[20] = static_cast<char>(bytes[20] ^ 0x01);  // inside the first record
    writeFile(path, bytes);
    EXPECT_THROW(RaftStorage{path}, CorruptLogError);
    EXPECT_EQ(readFile(path), bytes);  // nothing was "repaired"
}

// For every possible crash point in the Raft log:
//   1. recovery gives exactly the state after the last fully written update,
//   2. the half-written tail is cut off,
//   3. a new update after recovery survives the next reopen.
TEST(RaftStorage, RecoversFromCrashAtEveryByte) {
    kv::test::TempDir dir;
    const auto source = dir.file("source.log");

    const std::vector<StorageUpdate> updates = {
        update(HardState{1, 2u}),
        update(HardState{1, 2u}, {entry(1, 1, "a"), entry(1, 2, "bb")}),
        update(std::nullopt, {entry(1, 3, "ccc")}),
        update(HardState{2, std::nullopt}, {entry(2, 2, "new"), entry(2, 3)}),  // overwrites 2 and 3
        update(HardState{3, 1u}),
    };
    std::vector<std::size_t> updateEnds;
    {
        RaftStorage s(source, false);
        for (const auto& u : updates) {
            s.save(u);
            updateEnds.push_back(std::filesystem::file_size(source));
        }
    }
    const std::string full = readFile(source);

    for (std::size_t cut = 0; cut <= full.size(); ++cut) {
        SCOPED_TRACE("crash after " + std::to_string(cut) + " bytes");
        const auto path = dir.file("crash.log");
        writeFile(path, full.substr(0, cut));

        std::size_t complete = 0;
        while (complete < updates.size() && updateEnds[complete] <= cut) ++complete;
        const std::size_t goodBytes = complete ? updateEnds[complete - 1] : 0;
        const RaftState expected = model(updates, complete);

        {
            RaftStorage s(path, false);
            EXPECT_EQ(s.recovery().updatesReplayed, complete);
            EXPECT_EQ(s.recovery().truncatedTornTail, cut != goodBytes);
            EXPECT_EQ(std::filesystem::file_size(path), goodBytes);
            EXPECT_EQ(s.recovery().state.hardState, expected.hardState);
            EXPECT_EQ(s.recovery().state.log, expected.log);

            s.save(update(std::nullopt, {entry(99, expected.log.size() + 1, "after crash")}));
        }
        RaftStorage reopened(path, false);
        EXPECT_FALSE(reopened.recovery().truncatedTornTail);
        EXPECT_EQ(reopened.recovery().state.log.size(), expected.log.size() + 1);
        EXPECT_EQ(reopened.recovery().state.log.back().data, "after crash");
    }
}
