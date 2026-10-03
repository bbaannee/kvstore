// Snapshots in RaftNode: taking them, sending them to a lagging follower in
// chunks, and installing them.

#include <gtest/gtest.h>

#include "node_test_util.h"

using namespace kv::test;

namespace {

// A single-node cluster that has applied `commands` commands (indices 2..commands+1; index 1 is its no-op).
RaftNode singleNodeWith(int commands) {
    RaftNode n(config(1, {1}), {}, {});
    tickUntilCandidate(n);
    for (int i = 0; i < commands; ++i) n.propose("cmd" + std::to_string(i + 2));
    n.takeReady();  // everything is committed, applied and persisted
    return n;
}

Config smallChunks() {
    Config c = config(1);
    c.snapshotChunkBytes = 4;
    c.electionTimeoutTicks = 100;  // out of the way: these tests tick without answering
    return c;
}

}  // namespace

TEST(RaftNodeSnapshot, CompactDropsTheLogAndReportsTheSnapshot) {
    RaftNode n = singleNodeWith(5);  // log: no-op at 1, commands at 2..6
    ASSERT_EQ(n.lastApplied(), 6u);

    ASSERT_TRUE(n.compact(4, "the state at 4"));
    EXPECT_EQ(n.log().baseIndex(), 4u);
    EXPECT_EQ(n.snapshotIndex(), 4u);
    EXPECT_EQ(n.log().lastIndex(), 6u);

    Ready rd = n.takeReady();
    ASSERT_TRUE(rd.snapshot);
    EXPECT_EQ(rd.snapshot->index, 4u);
    EXPECT_EQ(rd.snapshot->term, n.term());
    EXPECT_EQ(rd.snapshot->data, "the state at 4");
    EXPECT_EQ(rd.snapshot->members, (Members{{1, ""}}));
    EXPECT_FALSE(rd.snapshotFromLeader);  // we made it ourselves: nothing to restore
    // The stored log is replaced by what follows the snapshot.
    ASSERT_EQ(rd.entries.size(), 2u);
    EXPECT_EQ(rd.entries[0].index, 5u);
    EXPECT_EQ(rd.entries[1].index, 6u);
    EXPECT_TRUE(rd.committedEntries.empty());  // compaction doesn't replay anything
}

TEST(RaftNodeSnapshot, CompactRefusesWhatCannotBeCompacted) {
    RaftNode n = singleNodeWith(5);
    EXPECT_FALSE(n.compact(7, "x"));  // beyond what was applied
    EXPECT_FALSE(n.compact(0, "x"));  // nothing to cover
    ASSERT_TRUE(n.compact(4, "x"));
    EXPECT_FALSE(n.compact(4, "x"));  // already covered
    EXPECT_FALSE(n.compact(3, "x"));  // older than the snapshot we have
    EXPECT_TRUE(n.compact(6, "y"));   // newer is fine

    RaftNode leader = leaderOfThree();  // nothing committed, so nothing applied
    leader.propose("a");
    EXPECT_FALSE(leader.compact(2, "x")) << "entries that aren't applied yet must stay in the log";
}

TEST(RaftNodeSnapshot, FollowerInstallsAChunkedSnapshot) {
    RaftNode n(config(1), {}, {});
    const Members members{{1, ""}, {2, ""}, {3, ""}};

    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, members, 0, "aaaa", false}));
    Ready rd = n.takeReady();
    EXPECT_FALSE(rd.snapshot);  // not complete yet
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(rd, 2), (InstallSnapshotResponse{20, 4, false}));
    EXPECT_EQ(n.leader(), 2u);  // a chunk is contact from the leader

    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, members, 4, "bbbb", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2), (InstallSnapshotResponse{20, 8, false}));

    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, members, 8, "cc", true}));
    rd = n.takeReady();
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(rd, 2), (InstallSnapshotResponse{20, 0, true}));
    ASSERT_TRUE(rd.snapshot);
    EXPECT_TRUE(rd.snapshotFromLeader);  // the state machine must be restored from it
    EXPECT_EQ(rd.snapshot->data, "aaaabbbbcc");
    EXPECT_EQ(rd.snapshot->index, 20u);
    EXPECT_EQ(rd.snapshot->members, members);
    EXPECT_TRUE(rd.entries.empty());

    EXPECT_EQ(n.log().baseIndex(), 20u);
    EXPECT_EQ(n.commitIndex(), 20u);
    EXPECT_EQ(n.lastApplied(), 20u);  // the state machine will be exactly at the snapshot
    EXPECT_TRUE(rd.committedEntries.empty());
}

TEST(RaftNodeSnapshot, ChunkProblemsAreAnsweredWithWhatIsNeeded) {
    RaftNode n(config(1), {}, {});
    const Members m{{1, ""}, {2, ""}, {3, ""}};

    // A chunk from the middle of a transfer we never saw the start of: ask for the beginning.
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 4, "bbbb", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2).nextOffset, 0u);

    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 0, "aaaa", false}));
    n.takeReady();
    // The same chunk again (a retransmission): still waiting for offset 4, and nothing is duplicated.
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 0, "aaaa", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2).nextOffset, 4u);

    // A chunk that skips ahead (one was lost): ask for the missing one.
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 8, "cccc", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2).nextOffset, 4u);

    // A duplicate of an older chunk in the middle: ignored, offset unchanged.
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 4, "bbbb", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2).nextOffset, 8u);
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 4, "bbbb", false}));
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(n.takeReady(), 2).nextOffset, 8u);

    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 8, "cc", true}));
    Ready rd = n.takeReady();
    ASSERT_TRUE(rd.snapshot);
    EXPECT_EQ(rd.snapshot->data, "aaaabbbbcc");
}

TEST(RaftNodeSnapshot, ANewTransferReplacesAnUnfinishedOne) {
    RaftNode n(config(1), {}, {});
    const Members m{{1, ""}, {2, ""}, {3, ""}};
    n.step(msg(2, 1, 1, InstallSnapshot{20, 1, m, 0, "old-", false}));
    n.takeReady();
    // The leader took a newer snapshot and starts over with it.
    n.step(msg(2, 1, 1, InstallSnapshot{30, 1, m, 0, "new!", true}));
    Ready rd = n.takeReady();
    ASSERT_TRUE(rd.snapshot);
    EXPECT_EQ(rd.snapshot->index, 30u);
    EXPECT_EQ(rd.snapshot->data, "new!");
}

TEST(RaftNodeSnapshot, ASnapshotWeAlreadyCoverIsAcknowledgedAndIgnored) {
    // Our log is committed up to 6; a snapshot at 5 tells us nothing new.
    RaftNode n(config(1), {1, std::nullopt}, {entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5), entry(1, 6)});
    n.step(msg(2, 1, 1, AppendEntries{0, 0, {}, 6}));  // learn that everything is committed
    n.takeReady();
    ASSERT_EQ(n.commitIndex(), 0u) << "(prev 0 with no entries commits nothing, as the leader sent no entries)";

    n.step(msg(2, 1, 1, AppendEntries{6, 1, {}, 6}));
    n.takeReady();
    ASSERT_EQ(n.commitIndex(), 6u);

    n.step(msg(2, 1, 1, InstallSnapshot{5, 1, {{1, ""}, {2, ""}, {3, ""}}, 0, "old", true}));
    Ready rd = n.takeReady();
    EXPECT_FALSE(rd.snapshot);
    EXPECT_EQ(bodyTo<InstallSnapshotResponse>(rd, 2), (InstallSnapshotResponse{5, 0, true}));
    EXPECT_EQ(n.log().lastIndex(), 6u);  // untouched
}

TEST(RaftNodeSnapshot, InstallingKeepsALogSuffixThatAgreesAndDropsOneThatDoesNot) {
    const Members m{{1, ""}, {2, ""}, {3, ""}};
    const std::vector<Entry> log = {entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4), entry(2, 5), entry(2, 6)};

    // The snapshot's entry 4 has term 2, like ours: entries 5 and 6 are still good.
    RaftNode agree(config(1), {2, std::nullopt}, log);
    agree.step(msg(2, 1, 2, InstallSnapshot{4, 2, m, 0, "state", true}));
    Ready rd = agree.takeReady();
    ASSERT_TRUE(rd.snapshot);
    ASSERT_EQ(rd.entries.size(), 2u);
    EXPECT_EQ(rd.entries[0].index, 5u);
    EXPECT_EQ(agree.log().lastIndex(), 6u);
    EXPECT_EQ(agree.commitIndex(), 4u);  // only up to the snapshot: 5 and 6 are not known to be committed

    // The snapshot's entry 4 has term 3: our log belongs to a history the cluster dropped.
    RaftNode conflict(config(1), {2, std::nullopt}, log);
    conflict.step(msg(2, 1, 3, InstallSnapshot{4, 3, m, 0, "state", true}));
    rd = conflict.takeReady();
    ASSERT_TRUE(rd.snapshot);
    EXPECT_TRUE(rd.entries.empty());
    EXPECT_EQ(conflict.log().lastIndex(), 4u);
}

TEST(RaftNodeSnapshot, AppendEntriesThatOverlapTheSnapshotAreTrimmed) {
    // Snapshot at 5; we hold entries 6 and 7.
    Snapshot snap{5, 1, {{1, ""}, {2, ""}, {3, ""}}, "state"};
    RaftNode n(config(1), {1, std::nullopt}, {entry(1, 6), entry(1, 7)}, snap);

    // The leader still thinks we need entries 4..8: the ones we have snapshotted are skipped, 8 is new.
    n.step(msg(2, 1, 1, AppendEntries{3, 1, {entry(1, 4), entry(1, 5), entry(1, 6), entry(1, 7), entry(1, 8)}, 8}));
    Ready rd = n.takeReady();
    EXPECT_EQ(bodyTo<AppendEntriesResponse>(rd, 2), (AppendEntriesResponse{true, 8, 0, 0}));
    EXPECT_EQ(n.log().lastIndex(), 8u);
    ASSERT_EQ(rd.entries.size(), 1u);
    EXPECT_EQ(rd.entries[0].index, 8u);

    // Everything in the message is covered by the snapshot: success, and we match up to the base.
    n.step(msg(2, 1, 1, AppendEntries{1, 1, {entry(1, 2), entry(1, 3)}, 8}));
    EXPECT_EQ(bodyTo<AppendEntriesResponse>(n.takeReady(), 2), (AppendEntriesResponse{true, 5, 0, 0}));
}

TEST(RaftNodeSnapshot, ARestartedNodeStartsFromItsSnapshot) {
    Snapshot snap{5, 2, {{1, "a:1"}, {2, "b:2"}, {3, "c:3"}}, "state"};
    RaftNode n(config(1), {2, std::nullopt}, {entry(2, 6)}, snap);

    EXPECT_EQ(n.commitIndex(), 5u);
    EXPECT_EQ(n.lastApplied(), 5u);
    EXPECT_EQ(n.log().baseIndex(), 5u);
    EXPECT_EQ(n.log().lastIndex(), 6u);
    EXPECT_EQ(n.members(), snap.members);  // the configuration comes from the snapshot
    EXPECT_EQ(n.configIndex(), 5u);
    EXPECT_FALSE(n.hasReady());  // nothing to re-persist, nothing to re-apply
}

TEST(RaftNodeSnapshot, LeaderSendsASnapshotToAFollowerBehindTheCompaction) {
    RaftNode n = leaderWith(smallChunks());
    const Term term = n.term();

    // Commit three entries (the no-op at 1, then commands 2 and 3) with both followers.
    n.propose("b");
    n.propose("c");
    ack(n, 2, 3);
    ack(n, 3, 3);
    n.takeReady();
    ASSERT_EQ(n.lastApplied(), 3u);
    ASSERT_TRUE(n.compact(3, "0123456789"));
    n.takeReady();

    // Follower 3 turns out to have nothing (say, it lost its disk).
    n.step(msg(3, 1, term, AppendEntriesResponse{false, 0, 0, 0}));
    Ready rd = n.takeReady();
    const auto& first = bodyTo<InstallSnapshot>(rd, 3);
    EXPECT_EQ(first.lastIndex, 3u);
    EXPECT_EQ(first.lastTerm, term);
    EXPECT_EQ(first.members, (Members{{1, ""}, {2, ""}, {3, ""}}));
    EXPECT_EQ(first.offset, 0u);
    EXPECT_EQ(first.data, "0123");
    EXPECT_FALSE(first.done);

    // While the transfer runs, new entries don't trigger more chunks.
    n.propose("d");
    EXPECT_FALSE(hasMessageTo<InstallSnapshot>(n.takeReady(), 3));

    n.step(msg(3, 1, term, InstallSnapshotResponse{3, 4, false}));
    rd = n.takeReady();
    EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).offset, 4u);
    EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).data, "4567");

    n.step(msg(3, 1, term, InstallSnapshotResponse{3, 8, false}));
    rd = n.takeReady();
    EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).data, "89");
    EXPECT_TRUE(bodyTo<InstallSnapshot>(rd, 3).done);

    // The follower has it all: ordinary replication resumes right after the snapshot.
    n.step(msg(3, 1, term, InstallSnapshotResponse{3, 0, true}));
    rd = n.takeReady();
    const auto& next = bodyTo<AppendEntries>(rd, 3);
    EXPECT_EQ(next.prevLogIndex, 3u);
    EXPECT_EQ(next.prevLogTerm, term);
    ASSERT_EQ(next.entries.size(), 1u);
    EXPECT_EQ(next.entries[0].index, 4u);  // "d"
}

TEST(RaftNodeSnapshot, ALostChunkIsResentAfterAPause) {
    RaftNode n = leaderWith(smallChunks());
    const Term term = n.term();
    n.propose("b");
    ack(n, 2, 2);
    ack(n, 3, 2);
    n.takeReady();
    ASSERT_TRUE(n.compact(2, "0123456789"));
    n.takeReady();
    n.step(msg(3, 1, term, AppendEntriesResponse{false, 0, 0, 0}));
    ASSERT_TRUE(hasMessageTo<InstallSnapshot>(n.takeReady(), 3));

    // Nobody answers. Heartbeats go on, and the chunk is retried only now and then, not on every heartbeat.
    int resends = 0;
    for (int i = 1; i <= 60; ++i) {
        n.tick();
        if (i % 3 == 0) ack(n, 2, 2);  // node 2 keeps answering
        Ready rd = n.takeReady();
        if (hasMessageTo<InstallSnapshot>(rd, 3)) {
            ++resends;
            EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).offset, 0u);
        }
    }
    EXPECT_GE(resends, 2);
    EXPECT_LE(resends, 5) << "a chunk that takes a while to acknowledge must not be re-sent on every heartbeat";
}

TEST(RaftNodeSnapshot, TakingANewSnapshotRestartsTransfersFromTheNewOne) {
    RaftNode n = leaderWith(smallChunks());
    const Term term = n.term();
    n.propose("b");
    n.propose("c");
    ack(n, 2, 3);
    ack(n, 3, 3);
    n.takeReady();
    ASSERT_TRUE(n.compact(2, "old-state!!"));
    n.takeReady();
    n.step(msg(3, 1, term, AppendEntriesResponse{false, 0, 0, 0}));
    n.takeReady();  // first chunk of the old snapshot is out

    ASSERT_TRUE(n.compact(3, "new-state"));
    n.takeReady();
    n.step(msg(3, 1, term, InstallSnapshotResponse{2, 4, false}));  // an answer about the OLD snapshot
    EXPECT_FALSE(hasMessageTo<InstallSnapshot>(n.takeReady(), 3)) << "answers about an obsolete snapshot are ignored";

    for (int i = 0; i < 20; ++i) n.tick();  // the retry timer restarts the transfer, now from the new snapshot
    Ready rd = n.takeReady();
    ASSERT_TRUE(hasMessageTo<InstallSnapshot>(rd, 3));
    EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).lastIndex, 3u);
    EXPECT_EQ(bodyTo<InstallSnapshot>(rd, 3).offset, 0u);
}
