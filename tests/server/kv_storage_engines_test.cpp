// The state machine on both storage engines: the same behaviour on each, and
// what only the LSM engine can do, which is remember its progress across a restart.

#include <map>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/coding.h"
#include "server/command.h"
#include "server/kv_state_machine.h"
#include "server/kv_storage.h"
#include "storage/log_record.h"
#include "temp_dir.h"

using kv::raft::Entry;
using kv::server::ApplyResult;
using kv::server::Command;
using kv::server::encodeCommand;
using kv::server::KvStateMachine;
using kv::storage::LogRecord;

namespace {

kv::lsm::Options tinyLsm(const std::string& dir) {
    kv::lsm::Options o;
    o.dir = dir;
    o.memtableBytes = 1500;  // tiny: flushes and compactions all the time
    o.blockBytes = 128;
    o.l0CompactionTrigger = 3;
    o.level1MaxBytes = 4000;
    o.levelMultiplier = 3;
    o.targetFileBytes = 800;
    o.syncWal = false;
    o.syncTables = false;
    return o;
}

Entry entry(kv::raft::Index index, std::uint64_t clientId, std::uint64_t seq, const LogRecord& rec) {
    return Entry{1, index, Entry::Type::Command, encodeCommand({clientId, seq, rec})};
}

Entry noop(kv::raft::Index index) { return Entry{1, index, Entry::Type::NoOp, {}}; }

// Which engine a test runs on, and a way to (re)open a state machine of that kind in a directory.
struct Engine {
    const char* name;
    bool persistent;
    std::unique_ptr<KvStateMachine> (*open)(const std::string& dir, std::size_t maxSessions);
};

std::unique_ptr<KvStateMachine> openMemory(const std::string&, std::size_t maxSessions) {
    return std::make_unique<KvStateMachine>(maxSessions);
}
std::unique_ptr<KvStateMachine> openLsm(const std::string& dir, std::size_t maxSessions) {
    return std::make_unique<KvStateMachine>(maxSessions, std::make_unique<kv::server::LsmStorage>(tinyLsm(dir)));
}

const Engine kEngines[] = {{"memory", false, openMemory}, {"lsm", true, openLsm}};

class Engines : public ::testing::TestWithParam<Engine> {
protected:
    std::unique_ptr<KvStateMachine> open(std::size_t maxSessions = 10'000) { return GetParam().open(dir_.file("state"), maxSessions); }
    kv::test::TempDir dir_;
};

}  // namespace

INSTANTIATE_TEST_SUITE_P(Storage, Engines, ::testing::ValuesIn(kEngines),
                         [](const auto& info) { return std::string(info.param.name); });

TEST_P(Engines, PutGetDelete) {
    auto sm = open();
    EXPECT_EQ(sm->apply(entry(1, 0, 0, LogRecord::put("a", "1"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm->apply(entry(2, 0, 0, LogRecord::get("a"))), (ApplyResult{true, "1"}));
    EXPECT_EQ(sm->apply(entry(3, 0, 0, LogRecord::put("a", "2"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm->get("a"), "2");
    EXPECT_EQ(sm->read("a"), (ApplyResult{true, "2"}));
    EXPECT_EQ(sm->apply(entry(4, 0, 0, LogRecord::del("a"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm->apply(entry(5, 0, 0, LogRecord::del("a"))), (ApplyResult{false, ""}));
    EXPECT_EQ(sm->apply(entry(6, 0, 0, LogRecord::get("a"))), (ApplyResult{false, ""}));
    EXPECT_EQ(sm->size(), 0u);

    for (int i = 0; i < 50; ++i) sm->apply(entry(7 + i, 0, 0, LogRecord::put("key" + std::to_string(i), std::string(i, 'v'))));
    EXPECT_EQ(sm->size(), 50u);
    EXPECT_EQ(sm->get("key17"), std::string(17, 'v'));
    EXPECT_EQ(sm->data().size(), 50u);
}

TEST_P(Engines, EveryEntryAdvancesTheAppliedIndex) {
    auto sm = open();
    EXPECT_EQ(sm->appliedIndex(), 0u);
    sm->apply(noop(1));  // entries Raft adds by itself change no data, but count as applied
    EXPECT_EQ(sm->appliedIndex(), 1u);
    sm->apply(entry(2, 0, 0, LogRecord::put("a", "1")));
    EXPECT_EQ(sm->appliedIndex(), 2u);
    sm->apply(Entry{1, 3, Entry::Type::Config, "whatever"});
    sm->apply(Entry{1, 4, Entry::Type::Command, "garbage that is not a command"});
    EXPECT_EQ(sm->appliedIndex(), 4u);
    EXPECT_EQ(sm->size(), 1u);
}

TEST_P(Engines, SessionsDeduplicateRetries) {
    auto sm = open();
    EXPECT_EQ(sm->apply(entry(1, 7, 1, LogRecord::put("k", "first"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm->apply(entry(2, 7, 2, LogRecord::del("k"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm->apply(entry(3, 7, 2, LogRecord::del("k"))), (ApplyResult{true, ""}));  // the retry gets the original answer
    EXPECT_EQ(sm->apply(entry(4, 7, 1, LogRecord::put("k", "STALE"))), ApplyResult{});   // an old duplicate: ignored
    EXPECT_EQ(sm->get("k"), std::nullopt);
    EXPECT_EQ(sm->sessionCount(), 1u);
}

TEST_P(Engines, SerializeAndRestoreRoundTrip) {
    auto a = open();
    a->apply(entry(1, 1, 1, LogRecord::put("k1", "v1")));
    a->apply(entry(2, 2, 1, LogRecord::put("k2", std::string("bin\0ary", 7))));
    a->apply(entry(3, 1, 2, LogRecord::get("k1")));
    a->apply(entry(4, 0, 0, LogRecord::put("", "empty key")));

    kv::test::TempDir other;
    auto b = GetParam().open(other.file("state"), 10'000);
    ASSERT_TRUE(b->restore(a->serialize(), 4));
    EXPECT_EQ(b->serialize(), a->serialize());
    EXPECT_EQ(b->appliedIndex(), 4u);
    EXPECT_EQ(b->sessionCount(), 2u);
    EXPECT_EQ(b->apply(entry(5, 1, 2, LogRecord::put("k1", "OVERWRITE"))), (ApplyResult{true, "v1"}));  // session restored
    EXPECT_EQ(b->get("k1"), "v1");
}

TEST_P(Engines, RestoreReplacesWhateverWasThere) {
    auto sm = open();
    for (int i = 0; i < 100; ++i) sm->apply(entry(i + 1, 0, 0, LogRecord::put("old" + std::to_string(i), "x")));
    kv::test::TempDir other;
    auto source = GetParam().open(other.file("state"), 10'000);
    source->apply(entry(1, 0, 0, LogRecord::put("new", "y")));

    ASSERT_TRUE(sm->restore(source->serialize(), 50));
    EXPECT_EQ(sm->size(), 1u);
    EXPECT_EQ(sm->get("new"), "y");
    EXPECT_EQ(sm->get("old5"), std::nullopt);
    EXPECT_EQ(sm->appliedIndex(), 50u);
}

TEST_P(Engines, AMalformedSnapshotLeavesTheStateEmpty) {
    auto sm = open();
    sm->apply(entry(1, 0, 0, LogRecord::put("a", "1")));
    kv::test::TempDir other;
    auto source = GetParam().open(other.file("state"), 10'000);
    source->apply(entry(1, 3, 1, LogRecord::put("x", "y")));
    const std::string good = source->serialize();

    for (std::size_t len = 0; len < good.size(); len += 3) {
        EXPECT_FALSE(sm->restore(std::string_view(good).substr(0, len), 9)) << len;
        EXPECT_EQ(sm->size(), 0u);
        EXPECT_EQ(sm->appliedIndex(), 0u) << "a failed restore must not claim progress";
    }
}

TEST_P(Engines, SessionsAreEvictedLeastRecentlyUsedFirst) {
    auto sm = open(/*maxSessions=*/3);
    sm->apply(entry(1, 1, 1, LogRecord::put("a", "1")));
    sm->apply(entry(2, 2, 1, LogRecord::put("b", "2")));
    sm->apply(entry(3, 3, 1, LogRecord::put("c", "3")));
    sm->apply(entry(4, 1, 2, LogRecord::put("a", "1b")));  // client 1 is the newest now
    sm->apply(entry(5, 4, 1, LogRecord::put("d", "4")));   // a 4th client: client 2 goes
    EXPECT_EQ(sm->sessionCount(), 3u);
    sm->apply(entry(6, 0, 0, LogRecord::del("b")));
    sm->apply(entry(7, 2, 1, LogRecord::put("b", "again")));  // forgotten, so executed again
    EXPECT_EQ(sm->get("b"), "again");
    sm->apply(entry(8, 1, 2, LogRecord::put("a", "SHOULD NOT RUN")));  // still remembered
    EXPECT_EQ(sm->get("a"), "1b");
}

// Both engines, fed the same history, must hold the same state: the replicas of a cluster may run different engines.
TEST(EnginesAgree, TheSameHistoryGivesTheSameSerializedState) {
    kv::test::TempDir dir;
    auto memory = openMemory("", 50);
    auto lsm = openLsm(dir.file("state"), 50);
    std::mt19937_64 rng(3);
    for (int i = 1; i <= 1500; ++i) {
        const std::string key = "key" + std::to_string(rng() % 120);
        const std::uint64_t client = rng() % 90;  // more clients than sessions: evictions happen
        LogRecord op = rng() % 3 == 0 ? LogRecord::del(key)
                       : rng() % 4 == 0 ? LogRecord::get(key)
                                        : LogRecord::put(key, "v" + std::to_string(i) + std::string(rng() % 30, 'x'));
        Entry e = entry(i, client, static_cast<std::uint64_t>(i), op);
        ASSERT_EQ(memory->apply(e), lsm->apply(e)) << "entry " << i;
    }
    EXPECT_EQ(memory->serialize(), lsm->serialize());
    EXPECT_EQ(memory->sessionCount(), lsm->sessionCount());
    EXPECT_EQ(lsm->appliedIndex(), 1500u);
}

// ---------------------------------------------------------------------------
// The LSM engine's own promise: it remembers where it got to.
// ---------------------------------------------------------------------------

TEST(LsmEngine, DataSessionsAndProgressSurviveARestart) {
    kv::test::TempDir dir;
    std::string before;
    {
        auto sm = openLsm(dir.file("state"), 20);
        std::mt19937_64 rng(8);
        for (int i = 1; i <= 800; ++i) {
            const std::string key = "key" + std::to_string(rng() % 100);
            sm->apply(entry(i, rng() % 40, static_cast<std::uint64_t>(i),
                            rng() % 4 == 0 ? LogRecord::del(key) : LogRecord::put(key, "v" + std::to_string(i))));
        }
        before = sm->serialize();
        ASSERT_EQ(sm->appliedIndex(), 800u);
    }
    auto reopened = openLsm(dir.file("state"), 20);
    EXPECT_EQ(reopened->appliedIndex(), 800u) << "it must know how far it got, so Raft doesn't replay the log";
    EXPECT_EQ(reopened->serialize(), before) << "data and sessions, including the order they would be evicted in";
}

// A group of applies is one atomic batch. If the process dies before the batch is committed, none of it is there.
TEST(LsmEngine, ABatchThatWasNeverCommittedLeavesNoTrace) {
    kv::test::TempDir dir;
    {
        auto sm = openLsm(dir.file("state"), 20);
        sm->apply(entry(1, 5, 1, LogRecord::put("committed", "yes")));

        sm->beginBatch();
        sm->apply(entry(2, 5, 2, LogRecord::put("lost", "no")));
        sm->apply(entry(3, 6, 1, LogRecord::put("lost2", "no")));
        EXPECT_EQ(sm->get("lost"), "no") << "reads inside the batch see its writes";
        // (destroyed without endBatch: the process dies in the middle of the batch)
    }
    auto reopened = openLsm(dir.file("state"), 20);
    EXPECT_EQ(reopened->appliedIndex(), 1u);
    EXPECT_EQ(reopened->get("committed"), "yes");
    EXPECT_EQ(reopened->get("lost"), std::nullopt);
    EXPECT_EQ(reopened->get("lost2"), std::nullopt);
    EXPECT_EQ(reopened->sessionCount(), 1u);
}

TEST(LsmEngine, NestedBatchesCommitOnceAtTheEnd) {
    kv::test::TempDir dir;
    {
        auto sm = openLsm(dir.file("state"), 20);
        sm->beginBatch();
        sm->beginBatch();
        sm->apply(entry(1, 0, 0, LogRecord::put("a", "1")));
        sm->endBatch();  // the inner one ends: nothing is committed yet
        sm->apply(entry(2, 0, 0, LogRecord::put("b", "2")));
        sm->endBatch();
    }
    auto reopened = openLsm(dir.file("state"), 20);
    EXPECT_EQ(reopened->appliedIndex(), 2u);
    EXPECT_EQ(reopened->get("b"), "2");
}

TEST(LsmEngine, MemoryNeverRemembersAnything) {
    kv::test::TempDir dir;
    auto memory = openMemory(dir.file("state"), 20);
    memory->apply(entry(1, 0, 0, LogRecord::put("a", "1")));
    EXPECT_EQ(memory->appliedIndex(), 1u);  // (within one run)
    auto fresh = openMemory(dir.file("state"), 20);
    EXPECT_EQ(fresh->appliedIndex(), 0u);   // after a restart it starts from the snapshot and the log
    EXPECT_EQ(fresh->size(), 0u);
}
