// ReplicatedNode features with real files: snapshots and log compaction,
// fast reads (ReadIndex), and membership changes.

#include <filesystem>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "replicated_cluster.h"
#include "storage/log_reader.h"

using kv::raft::Member;
using kv::raft::Members;
using kv::server::ApplyResult;
using kv::server::Command;
using kv::server::RequestStatus;
using kv::storage::LogRecord;
using kv::test::ReplicatedCluster;

namespace {

ReplicatedCluster::Options withSnapshots(std::size_t threshold, std::uint64_t seed = 1) {
    ReplicatedCluster::Options o;
    o.seed = seed;
    o.snapshotThreshold = threshold;
    return o;
}

// The write-ahead log file of a node.
std::uintmax_t logFileSize(ReplicatedCluster& c, ReplicatedCluster::NodeId id) {
    return std::filesystem::file_size(c.dataDir(id) + "/raft.log");
}

// Overwrites the same 10 keys over and over: the state stays small while the log keeps growing.
void writeWorkload(ReplicatedCluster& c, int writes) {
    for (int i = 0; i < writes; ++i) {
        auto t = c.submitToLeader(LogRecord::put("key" + std::to_string(i % 10), "value-number-" + std::to_string(i)));
        ASSERT_TRUE(t && c.wait(t));
    }
}

bool sameEverywhere(ReplicatedCluster& c) {
    const std::string reference = c.node(c.ids()[0]).state().serialize();
    for (auto id : c.ids())
        if (c.isUp(id) && c.node(id).state().serialize() != reference) return false;
    return true;
}

}  // namespace

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

TEST(ReplicatedNodeSnapshots, TheLogFileStopsGrowing) {
    ReplicatedCluster plain(3, withSnapshots(0));
    ReplicatedCluster compacting(3, withSnapshots(10));
    plain.waitForLeader();
    compacting.waitForLeader();
    writeWorkload(plain, 300);
    writeWorkload(compacting, 300);
    plain.tick(20);
    compacting.tick(20);

    for (auto id : compacting.ids()) {
        EXPECT_GT(logFileSize(plain, id), 12000u);                     // keeps every entry ever written
        EXPECT_LT(logFileSize(compacting, id), 4000u) << "node " << id;  // a snapshot and a short tail
        EXPECT_GT(compacting.node(id).raft().snapshotIndex(), 250u);
    }
}

TEST(ReplicatedNodeSnapshots, EveryNodeRestartsFromItsSnapshotAndLog) {
    ReplicatedCluster c(3, withSnapshots(8));
    c.waitForLeader();
    writeWorkload(c, 100);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    const std::string before = c.node(1).state().serialize();
    ASSERT_GT(c.node(1).raft().snapshotIndex(), 0u);

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) {
        c.restart(id);
        // The state is already there (from the snapshot), before any log entry is replayed.
        EXPECT_GT(c.node(id).appliedIndex(), 0u);
        EXPECT_GT(c.node(id).state().size(), 0u);
    }
    c.waitForLeader();
    EXPECT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_EQ(c.node(1).state().serialize(), before);

    // And the cluster carries on, from that state.
    auto t = c.submitToLeader(LogRecord::get("key3"));
    ASSERT_TRUE(t && c.wait(t));
    EXPECT_TRUE(t->result.found);
}

TEST(ReplicatedNodeSnapshots, AFollowerFarBehindIsCaughtUpBySnapshot) {
    ReplicatedCluster c(3, withSnapshots(10));
    auto leader = c.waitForLeader();
    const auto lagging = leader % 3 + 1;
    c.isolate(lagging);
    writeWorkload(c, 150);
    ASSERT_GT(c.node(leader).raft().snapshotIndex(), 100u);
    EXPECT_EQ(c.node(lagging).state().size(), 0u);

    c.heal();
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_EQ(c.node(lagging).state().size(), 10u);
    EXPECT_GT(c.node(lagging).raft().snapshotIndex(), 100u) << "it should have received a snapshot, not replayed 150 entries";

    // The snapshot it received was persisted: it restarts from it.
    c.crash(lagging);
    c.restart(lagging);
    EXPECT_GT(c.node(lagging).raft().snapshotIndex(), 100u);
    EXPECT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
}

// Retries must stay recognizable across snapshots: the session table is part of the snapshot.
TEST(ReplicatedNodeSnapshots, SessionsTravelInSnapshots) {
    ReplicatedCluster c(3, withSnapshots(5));
    auto leader = c.waitForLeader();
    const auto lagging = leader % 3 + 1;
    c.isolate(lagging);

    // A client makes a request; many other writes follow, so the leader snapshots past it.
    auto first = c.submit(leader, Command{42, 1, LogRecord::put("k", "first")});
    ASSERT_TRUE(c.wait(first));
    writeWorkload(c, 60);

    c.heal();
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_EQ(c.node(lagging).state().sessionCount(), c.node(leader).state().sessionCount());
    EXPECT_GE(c.node(lagging).state().sessionCount(), 1u);

    // Everyone agrees that client 42's request 1 was already done: a retry doesn't run again.
    auto retry = c.submit(*c.leader(), Command{42, 1, LogRecord::put("k", "RETRY MUST NOT RUN")});
    ASSERT_TRUE(c.wait(retry));
    EXPECT_EQ(retry->status, RequestStatus::Committed);
    for (auto id : c.ids()) EXPECT_EQ(c.node(id).state().get("k"), "first") << "node " << id;
}

TEST(ReplicatedNodeSnapshots, SnapshotNowOnDemand) {
    ReplicatedCluster c(3, withSnapshots(0));
    c.waitForLeader();
    writeWorkload(c, 20);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));

    auto leader = *c.leader();
    ASSERT_TRUE(c.node(leader).snapshotNow());
    EXPECT_EQ(c.node(leader).raft().snapshotIndex(), c.node(leader).appliedIndex());
    EXPECT_FALSE(c.node(leader).snapshotNow()) << "nothing new to cover";
}

// ---------------------------------------------------------------------------
// ReadIndex reads
// ---------------------------------------------------------------------------

TEST(ReplicatedNodeReads, ReadsSeeEveryAcknowledgedWrite) {
    ReplicatedCluster c(3);
    c.waitForLeader();

    for (int i = 0; i < 20; ++i) {
        auto put = c.submitToLeader(LogRecord::put("x", std::to_string(i)));
        ASSERT_TRUE(put && c.wait(put));
        auto read = c.read(*c.leader(), "x");
        ASSERT_TRUE(c.wait(read));
        ASSERT_EQ(read->status, RequestStatus::Committed);
        EXPECT_EQ(read->result, (ApplyResult{true, std::to_string(i)})) << "a read after an acknowledged write must see it";
    }
    auto missing = c.read(*c.leader(), "nothing");
    ASSERT_TRUE(c.wait(missing));
    EXPECT_EQ(missing->result, ApplyResult{});
}

TEST(ReplicatedNodeReads, FollowersRefuseReads) {
    ReplicatedCluster c(3);
    auto leader = c.waitForLeader();
    for (auto id : c.ids()) {
        if (id == leader) continue;
        auto t = c.read(id, "x");
        ASSERT_TRUE(t->done);  // answered immediately
        EXPECT_EQ(t->status, RequestStatus::NotLeader);
    }
}

TEST(ReplicatedNodeReads, ReadsDoNotAddToTheLog) {
    ReplicatedCluster c(3);
    auto leader = c.waitForLeader();
    auto put = c.submitToLeader(LogRecord::put("x", "1"));
    ASSERT_TRUE(c.wait(put));
    const auto last = c.node(leader).raft().log().lastIndex();

    for (int i = 0; i < 50; ++i) {
        auto t = c.read(leader, "x");
        ASSERT_TRUE(c.wait(t));
    }
    EXPECT_EQ(c.node(leader).raft().log().lastIndex(), last) << "a read must not cost a log entry";
}

TEST(ReplicatedNodeReads, ASingleNodeServesReads) {
    ReplicatedCluster c(1);
    c.waitForLeader();
    auto put = c.submitToLeader(LogRecord::put("a", "b"));
    ASSERT_TRUE(c.wait(put));
    auto t = c.read(1, "a");
    ASSERT_TRUE(c.wait(t));
    EXPECT_EQ(t->result, (ApplyResult{true, "b"}));
}

// ---------------------------------------------------------------------------
// Membership
// ---------------------------------------------------------------------------

namespace {

ReplicatedCluster::Options bootstrapped(std::size_t snapshotThreshold = 0) {
    ReplicatedCluster::Options o;
    o.bootstrapConfig = true;
    o.snapshotThreshold = snapshotThreshold;
    return o;
}

}  // namespace

TEST(ReplicatedNodeMembership, AddingAServerCommitsAndTheServerCatchesUp) {
    ReplicatedCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    writeWorkload(c, 30);

    c.addNode(4);
    Members members = c.node(leader).raft().members();
    members.push_back(Member{4, ""});
    auto change = c.changeMembership(leader, members);
    ASSERT_TRUE(c.wait(change));
    EXPECT_EQ(change->status, RequestStatus::Committed);

    EXPECT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_TRUE(c.node(4).raft().isMember());
    EXPECT_EQ(c.node(4).state().size(), 10u);
}

TEST(ReplicatedNodeMembership, RequestsThatCannotBeHonouredAreRejected) {
    ReplicatedCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    c.tick(10);
    const Members current = c.node(leader).raft().members();

    // A follower can't change the configuration.
    for (auto id : c.ids()) {
        if (id == leader) continue;
        auto t = c.changeMembership(id, current);
        ASSERT_TRUE(t->done);
        EXPECT_EQ(t->status, RequestStatus::NotLeader);
    }
    // Two changes at once, or none at all: invalid.
    Members two = current;
    two.push_back(Member{4, ""});
    two.push_back(Member{5, ""});
    auto invalid = c.changeMembership(leader, two);
    ASSERT_TRUE(invalid->done);
    EXPECT_EQ(invalid->status, RequestStatus::Rejected);
    auto nothing = c.changeMembership(leader, current);
    ASSERT_TRUE(nothing->done);
    EXPECT_EQ(nothing->status, RequestStatus::Rejected);

    // A change while another is still uncommitted: also rejected. (Cut the leader off so the first can't commit.)
    c.addNode(4);
    c.isolate(leader);
    Members three = current;
    three.push_back(Member{4, ""});
    auto first = c.changeMembership(leader, three);
    EXPECT_FALSE(first->done);  // appended, waiting for a majority it can't reach
    Members four = three;
    four.push_back(Member{5, ""});
    auto second = c.changeMembership(leader, four);
    ASSERT_TRUE(second->done);
    EXPECT_EQ(second->status, RequestStatus::Rejected);
}

TEST(ReplicatedNodeMembership, RemovingAServerAndTheClusterKeepsServing) {
    ReplicatedCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    const auto victim = leader % 3 + 1;
    c.tick(10);

    Members members = c.node(leader).raft().members();
    members.erase(std::remove_if(members.begin(), members.end(), [&](const Member& m) { return m.id == victim; }), members.end());
    auto change = c.changeMembership(leader, members);
    ASSERT_TRUE(c.wait(change));
    EXPECT_EQ(change->status, RequestStatus::Committed);

    auto put = c.submitToLeader(LogRecord::put("after", "removal"));
    ASSERT_TRUE(put && c.wait(put));
    EXPECT_EQ(put->status, RequestStatus::Committed);
}

TEST(ReplicatedNodeMembership, AServerAddedLateIsCaughtUpWithASnapshot) {
    ReplicatedCluster c(3, bootstrapped(/*snapshotThreshold=*/10));
    auto leader = c.waitForLeader();
    writeWorkload(c, 80);
    ASSERT_GT(c.node(leader).raft().snapshotIndex(), 50u);

    c.addNode(4);
    Members members = c.node(leader).raft().members();
    members.push_back(Member{4, ""});
    auto change = c.changeMembership(leader, members);
    ASSERT_TRUE(c.wait(change));
    EXPECT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_GT(c.node(4).raft().snapshotIndex(), 50u);
}

// A restarted server remembers the configuration (from its log or snapshot), not just the one on the command line.
TEST(ReplicatedNodeMembership, TheConfigurationSurvivesRestarts) {
    ReplicatedCluster c(3, bootstrapped(/*snapshotThreshold=*/8));
    auto leader = c.waitForLeader();
    c.addNode(4);
    Members members = c.node(leader).raft().members();
    members.push_back(Member{4, ""});
    auto change = c.changeMembership(leader, members);
    ASSERT_TRUE(c.wait(change));
    writeWorkload(c, 40);  // snapshots now cover the configuration entry
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) c.restart(id);
    for (auto id : c.ids()) EXPECT_EQ(c.node(id).raft().members().size(), 4u) << "node " << id;
    c.waitForLeader();
    auto put = c.submitToLeader(LogRecord::put("after", "restart"));
    ASSERT_TRUE(put && c.wait(put));
}

// ---------------------------------------------------------------------------
// The LSM engine inside the replicated store
// ---------------------------------------------------------------------------

namespace {

ReplicatedCluster::Options onLsm(std::size_t snapshotThreshold = 0) {
    ReplicatedCluster::Options o;
    o.engine = kv::server::ReplicatedNode::Engine::Lsm;
    o.snapshotThreshold = snapshotThreshold;
    return o;
}

}  // namespace

// The point of a persistent engine: after a restart the node already has its data, and Raft
// resumes from where the state machine got to, instead of replaying the whole log.
TEST(ReplicatedNodeLsm, ARestartedNodeResumesWhereItsStateMachineStopped) {
    ReplicatedCluster c(3, onLsm());
    c.waitForLeader();
    writeWorkload(c, 100);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    const auto applied = c.node(1).appliedIndex();
    ASSERT_GT(applied, 90u);

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) {
        c.restart(id);
        EXPECT_EQ(c.node(id).appliedIndex(), applied) << "node " << id << " would replay its whole log";
        EXPECT_EQ(c.node(id).state().size(), 10u);  // the data is there before a single message was handled
        EXPECT_EQ(c.node(id).raft().lastApplied(), applied);
    }
    c.waitForLeader();
    auto t = c.submitToLeader(LogRecord::get("key3"));
    ASSERT_TRUE(t && c.wait(t));
    EXPECT_TRUE(t->result.found);
    EXPECT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
}

TEST(ReplicatedNodeLsm, TheMemoryEngineReplaysAfterARestartWhereLsmDoesNot) {
    ReplicatedCluster c(3, withSnapshots(0));  // memory engine, no snapshots
    c.waitForLeader();
    writeWorkload(c, 50);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    c.crash(1);
    c.restart(1);
    EXPECT_EQ(c.node(1).appliedIndex(), 0u);  // everything is rebuilt from the log as the cluster re-commits it
    EXPECT_EQ(c.node(1).state().size(), 0u);
}

TEST(ReplicatedNodeLsm, AFollowerFarBehindIsCaughtUpBySnapshot) {
    ReplicatedCluster c(3, onLsm(10));
    auto leader = c.waitForLeader();
    const auto lagging = leader % 3 + 1;
    c.isolate(lagging);
    writeWorkload(c, 150);
    c.heal();
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    EXPECT_EQ(c.node(lagging).state().size(), 10u);
    EXPECT_GT(c.node(lagging).raft().snapshotIndex(), 100u);

    c.crash(lagging);
    c.restart(lagging);
    EXPECT_EQ(c.node(lagging).state().size(), 10u);  // the installed snapshot went into the LSM tree, durably
}

// If the LSM directory is lost (a replaced disk, a bad restore), the node falls back to its Raft snapshot and log.
TEST(ReplicatedNodeLsm, ALostStateDirectoryIsRebuiltFromTheRaftSnapshotAndLog) {
    ReplicatedCluster c(3, onLsm(10));
    c.waitForLeader();
    writeWorkload(c, 120);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));
    const std::string expected = c.node(1).state().serialize();

    c.crash(2);
    std::filesystem::remove_all(c.dataDir(2) + "/state");
    c.restart(2);
    EXPECT_EQ(c.node(2).state().size(), 10u) << "restored from the Raft snapshot";
    EXPECT_TRUE(c.tickUntil([&] { return c.node(2).state().serialize() == expected; }));
}

// The state machine can't have applied entries the Raft log no longer holds: that would mean the
// log was damaged or replaced, and carrying on would be building on a lie.
TEST(ReplicatedNodeLsm, AStateMachineAheadOfItsRaftLogIsRefused) {
    ReplicatedCluster c(3, onLsm());
    c.waitForLeader();
    writeWorkload(c, 30);
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));

    c.crash(3);
    std::filesystem::remove(c.dataDir(3) + "/raft.log");  // the log is gone, the LSM tree remembers
    EXPECT_THROW(c.restart(3), kv::storage::CorruptLogError);
}

TEST(ReplicatedNodeLsm, SessionsSurviveARestartSoRetriesStayHarmless) {
    ReplicatedCluster c(3, onLsm());
    auto leader = c.waitForLeader();
    auto first = c.submit(leader, Command{42, 1, LogRecord::put("k", "first")});
    ASSERT_TRUE(c.wait(first));
    ASSERT_TRUE(c.tickUntil([&] { return sameEverywhere(c); }));

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) c.restart(id);
    for (auto id : c.ids()) EXPECT_EQ(c.node(id).state().sessionCount(), 1u) << "node " << id;
    c.waitForLeader();

    auto retry = c.submit(*c.leader(), Command{42, 1, LogRecord::put("k", "RETRY MUST NOT RUN")});
    ASSERT_TRUE(c.wait(retry));
    for (auto id : c.ids()) EXPECT_EQ(c.node(id).state().get("k"), "first") << "node " << id;
}
