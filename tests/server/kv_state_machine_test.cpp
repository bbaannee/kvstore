#include "server/kv_state_machine.h"

#include <gtest/gtest.h>

#include "common/coding.h"
#include "server/command.h"
#include "storage/log_record.h"

using kv::raft::Entry;
using kv::server::ApplyResult;
using kv::server::Command;
using kv::server::encodeCommand;
using kv::server::KvStateMachine;
using kv::storage::LogRecord;

namespace {

// An entry from an anonymous client (no deduplication).
Entry command(const LogRecord& rec) { return Entry{1, 1, Entry::Type::Command, encodeCommand({0, 0, rec})}; }

// An entry from client `clientId`, request number `seq`.
Entry fromClient(std::uint64_t clientId, std::uint64_t seq, const LogRecord& rec) {
    return Entry{1, 1, Entry::Type::Command, encodeCommand({clientId, seq, rec})};
}

}  // namespace

TEST(KvStateMachine, AppliesPutGetDel) {
    KvStateMachine sm;
    EXPECT_EQ(sm.apply(command(LogRecord::put("a", "1"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm.apply(command(LogRecord::get("a"))), (ApplyResult{true, "1"}));
    EXPECT_EQ(sm.apply(command(LogRecord::put("a", "2"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm.get("a"), "2");
    EXPECT_EQ(sm.apply(command(LogRecord::del("a"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm.apply(command(LogRecord::del("a"))), (ApplyResult{false, ""}));
    EXPECT_EQ(sm.apply(command(LogRecord::get("a"))), (ApplyResult{false, ""}));
    EXPECT_EQ(sm.size(), 0u);
}

TEST(KvStateMachine, ReadsDoNotChangeAnything) {
    KvStateMachine sm;
    sm.apply(command(LogRecord::put("a", "1")));
    sm.apply(command(LogRecord::get("a")));
    sm.apply(command(LogRecord::get("missing")));
    EXPECT_EQ(sm.size(), 1u);
}

TEST(KvStateMachine, NoOpAndGarbageChangeNothing) {
    KvStateMachine sm;
    EXPECT_EQ(sm.apply(Entry{1, 1, Entry::Type::NoOp, {}}), ApplyResult{});
    // A malformed command is ignored (identically on every replica).
    EXPECT_EQ(sm.apply(Entry{1, 2, Entry::Type::Command, "not a command"}), ApplyResult{});
    EXPECT_EQ(sm.apply(Entry{1, 3, Entry::Type::Command, ""}), ApplyResult{});
    EXPECT_EQ(sm.size(), 0u);
}

TEST(KvStateMachine, CommandEncodingRoundTrips) {
    for (const Command& c : {Command{0, 0, LogRecord::put("k", "v")}, Command{7, 3, LogRecord::del("key")},
                             Command{~0ull, ~0ull, LogRecord::get("")}}) {
        Command out;
        ASSERT_TRUE(kv::server::decodeCommand(encodeCommand(c), out));
        EXPECT_EQ(out, c);
    }
    Command out;
    EXPECT_FALSE(kv::server::decodeCommand("short", out));
}

// A retry of the same request returns the original answer and does not run twice.
TEST(KvStateMachine, RetryOfTheSameRequestIsNotRunTwice) {
    KvStateMachine sm;
    EXPECT_EQ(sm.apply(fromClient(9, 1, LogRecord::put("k", "1"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm.apply(fromClient(9, 2, LogRecord::del("k"))), (ApplyResult{true, ""}));  // k existed
    ASSERT_EQ(sm.size(), 0u);

    // The Del is retried (say its answer was lost). Run twice it would answer
    // "not found"; deduplicated, it repeats the original answer.
    EXPECT_EQ(sm.apply(fromClient(9, 2, LogRecord::del("k"))), (ApplyResult{true, ""}));
}

// The dangerous case: an old request shows up late, after newer ones.
TEST(KvStateMachine, StaleDuplicateDoesNotOverwriteNewerData) {
    KvStateMachine sm;
    sm.apply(fromClient(9, 1, LogRecord::put("k", "old")));
    sm.apply(fromClient(9, 2, LogRecord::put("k", "new")));
    sm.apply(fromClient(9, 1, LogRecord::put("k", "old")));  // delayed copy of request 1
    EXPECT_EQ(sm.get("k"), "new");
}

TEST(KvStateMachine, SerializeAndRestoreRoundTrip) {
    KvStateMachine a;
    a.apply(fromClient(1, 1, LogRecord::put("k1", "v1")));
    a.apply(fromClient(2, 1, LogRecord::put("k2", std::string("bin\0ary", 7))));
    a.apply(fromClient(1, 2, LogRecord::get("k1")));
    a.apply(command(LogRecord::put("", "empty key")));

    KvStateMachine b;
    ASSERT_TRUE(b.restore(a.serialize(), 0));
    EXPECT_EQ(b.size(), a.size());
    for (const auto& [k, v] : a.data()) EXPECT_EQ(b.get(k), v);
    EXPECT_EQ(b.sessionCount(), 2u);
    EXPECT_EQ(b.serialize(), a.serialize());  // equal states give equal bytes

    // The restored sessions still deduplicate: client 1's last request was seq 2 (a get of "k1").
    EXPECT_EQ(b.apply(fromClient(1, 2, LogRecord::put("k1", "OVERWRITE"))), (ApplyResult{true, "v1"}));
    EXPECT_EQ(b.get("k1"), "v1");
}

// Replicas must produce identical snapshots no matter how their hash tables happen to be laid out.
TEST(KvStateMachine, SerializationIsDeterministic) {
    KvStateMachine a, b;
    for (int i = 0; i < 50; ++i) a.apply(command(LogRecord::put("key" + std::to_string(i), "v")));
    for (int i = 49; i >= 0; --i) b.apply(command(LogRecord::put("key" + std::to_string(i), "v")));  // another insertion order
    EXPECT_EQ(a.serialize(), b.serialize());
}

TEST(KvStateMachine, RestoreRejectsMalformedBytesAndStaysEmpty) {
    KvStateMachine a;
    a.apply(fromClient(1, 1, LogRecord::put("key", "value")));
    const std::string good = a.serialize();

    for (std::size_t len = 0; len < good.size(); ++len) {
        KvStateMachine b;
        b.apply(command(LogRecord::put("old", "data")));
        EXPECT_FALSE(b.restore(std::string_view(good).substr(0, len), 0)) << "length " << len;
        EXPECT_EQ(b.size(), 0u) << "a failed restore must not leave half a state";
    }
    KvStateMachine c;
    EXPECT_FALSE(c.restore(good + "x", 0));
    std::string bad = good;
    bad[0] = 'X';
    EXPECT_FALSE(c.restore(bad, 0));

    // A map size claiming billions of items in a tiny buffer.
    std::string huge;
    huge += static_cast<char>(0x4B);
    kv::putFixed64(huge, 0);
    kv::putFixed64(huge, ~0ull);
    EXPECT_FALSE(c.restore(huge, 0));
}

// The session table is bounded. Which client gets forgotten must depend only
// on the order of applied commands, so every replica forgets the same one.
TEST(KvStateMachine, SessionTableEvictsTheLeastRecentlyUsedClient) {
    KvStateMachine sm(/*maxSessions=*/3);
    sm.apply(fromClient(1, 1, LogRecord::put("a", "1")));
    sm.apply(fromClient(2, 1, LogRecord::put("b", "2")));
    sm.apply(fromClient(3, 1, LogRecord::put("c", "3")));
    sm.apply(fromClient(1, 2, LogRecord::put("a", "1b")));  // client 1 is used again: it's now the newest
    EXPECT_EQ(sm.sessionCount(), 3u);

    sm.apply(fromClient(4, 1, LogRecord::put("d", "4")));   // a 4th client: client 2 (least recently used) goes
    EXPECT_EQ(sm.sessionCount(), 3u);

    // Client 2 was forgotten, so a retry of its request is executed again (this is why the limit must be generous).
    sm.apply(command(LogRecord::del("b")));
    EXPECT_EQ(sm.apply(fromClient(2, 1, LogRecord::put("b", "again"))), (ApplyResult{true, ""}));
    EXPECT_EQ(sm.get("b"), "again");

    // Client 1 is still remembered, so its retry is not executed.
    sm.apply(fromClient(4, 2, LogRecord::put("d", "4b")));
    sm.apply(fromClient(1, 2, LogRecord::put("a", "SHOULD NOT RUN")));
    EXPECT_EQ(sm.get("a"), "1b");
}

TEST(KvStateMachine, EvictionOrderSurvivesASnapshot) {
    KvStateMachine a(2);
    a.apply(fromClient(1, 1, LogRecord::put("a", "1")));
    a.apply(fromClient(2, 1, LogRecord::put("b", "2")));

    KvStateMachine b(2);
    ASSERT_TRUE(b.restore(a.serialize(), 0));
    // Both continue identically: client 3 evicts client 1 in both.
    a.apply(fromClient(3, 1, LogRecord::put("c", "3")));
    b.apply(fromClient(3, 1, LogRecord::put("c", "3")));
    EXPECT_EQ(a.serialize(), b.serialize());
}

TEST(KvStateMachine, ClientsAreIndependentAndAnonymousIsNeverDeduplicated) {
    KvStateMachine sm;
    sm.apply(fromClient(1, 5, LogRecord::put("a", "x")));
    sm.apply(fromClient(2, 1, LogRecord::put("b", "y")));  // another client may reuse low seqs
    EXPECT_EQ(sm.size(), 2u);
    EXPECT_EQ(sm.sessionCount(), 2u);

    sm.apply(command(LogRecord::put("c", "1")));
    sm.apply(command(LogRecord::put("c", "2")));  // same (0, 0) twice: both run
    EXPECT_EQ(sm.get("c"), "2");
    EXPECT_EQ(sm.sessionCount(), 2u);
}
