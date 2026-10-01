// Whole-cluster scenarios: elections, replication, crashes, partitions.
// TestCluster checks the safety guarantees continuously during every test.

#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test_cluster.h"

using kv::test::TestCluster;

namespace {

// True once every running node has applied exactly `expected`.
bool allApplied(TestCluster& c, const std::vector<std::string>& expected) {
    for (auto id : c.ids())
        if (c.isUp(id) && c.applied(id) != expected) return false;
    return true;
}

}  // namespace

TEST(RaftCluster, ElectsOneStableLeader) {
    for (std::size_t size : {1u, 3u, 5u}) {
        SCOPED_TRACE("cluster of " + std::to_string(size));
        TestCluster c(size);
        auto leader = c.waitForLeader();
        auto term = c.node(leader).term();

        c.tick(200);  // with no failures, nothing should change
        EXPECT_EQ(c.leader(), leader);
        EXPECT_EQ(c.node(leader).term(), term);
        for (auto id : c.ids()) EXPECT_EQ(c.node(id).leader(), leader);
    }
}

TEST(RaftCluster, ReplicatesCommandsInOrder) {
    TestCluster c(3);
    c.waitForLeader();
    std::vector<std::string> cmds;
    for (int i = 0; i < 20; ++i) {
        cmds.push_back("cmd" + std::to_string(i));
        ASSERT_TRUE(c.propose(cmds.back()));
    }
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));
}

TEST(RaftCluster, SurvivesLeaderCrash) {
    TestCluster c(3);
    auto oldLeader = c.waitForLeader();
    ASSERT_TRUE(c.propose("before"));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, {"before"}); }));

    c.crash(oldLeader);
    auto newLeader = c.waitForLeader();
    EXPECT_NE(newLeader, oldLeader);
    ASSERT_TRUE(c.propose("after"));
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, {"before", "after"}); }));

    // The old leader comes back as a follower and catches up.
    c.restart(oldLeader);
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, {"before", "after"}); }));
    EXPECT_EQ(c.node(oldLeader).role(), kv::raft::Role::Follower);
}

TEST(RaftCluster, MinorityLeaderCannotCommit) {
    TestCluster c(5);
    auto oldLeader = c.waitForLeader();
    ASSERT_TRUE(c.propose("committed"));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, {"committed"}); }));

    // Cut the leader off with one follower: 2 of 5 is not a majority.
    std::set<kv::raft::NodeId> minority{oldLeader, oldLeader % 5 + 1};
    c.partition(minority);
    ASSERT_TRUE(c.node(oldLeader).propose("lost").has_value());  // accepted, never committed
    c.tick(100);
    for (auto id : minority) EXPECT_EQ(c.applied(id), std::vector<std::string>{"committed"});

    // The majority side elects its own leader and keeps going.
    std::optional<kv::raft::NodeId> newLeader;
    ASSERT_TRUE(c.tickUntil([&] {
        for (auto id : c.ids())
            if (!minority.count(id) && c.node(id).role() == kv::raft::Role::Leader) newLeader = id;
        return newLeader.has_value();
    }));
    ASSERT_TRUE(c.node(*newLeader).propose("kept").has_value());

    // After healing, the old leader's uncommitted entry is overwritten.
    c.heal();
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, {"committed", "kept"}); }));
}

TEST(RaftCluster, LaggingFollowerCatchesUp) {
    TestCluster c(3);
    auto leader = c.waitForLeader();
    auto lagging = leader % 3 + 1;
    c.isolate(lagging);  // the other two still form a majority and keep committing

    std::vector<std::string> cmds;
    for (int i = 0; i < 200; ++i) {  // more than one message's worth (64 entries)
        cmds.push_back("c" + std::to_string(i));
        ASSERT_TRUE(c.propose(cmds.back()));
    }
    c.tick(50);
    EXPECT_TRUE(c.applied(lagging).empty());

    c.heal();
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));
}

TEST(RaftCluster, WholeClusterRestartKeepsData) {
    TestCluster c(3);
    c.waitForLeader();
    ASSERT_TRUE(c.propose("a"));
    ASSERT_TRUE(c.propose("b"));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, {"a", "b"}); }));

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) c.restart(id);
    c.waitForLeader();
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, {"a", "b"}); }));
}

// Random chaos: message loss, reordering, crashes, restarts and partitions,
// many seeds. TestCluster fails the test on any safety violation. At the
// end the network heals and every node must converge to the same state.
TEST(RaftCluster, SafeUnderRandomFailures) {
    for (std::uint64_t seed = 1; seed <= 30; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        TestCluster c(5, seed);
        std::mt19937_64 rng(seed);
        c.setDropRate(0.1);

        int proposed = 0;
        for (int step = 0; step < 300; ++step) {
            switch (rng() % 20) {
                case 0: {  // crash a random node, but keep a majority running
                    auto id = c.ids()[rng() % 5];
                    int up = 0;
                    for (auto n : c.ids()) up += c.isUp(n);
                    if (c.isUp(id) && up > 3) c.crash(id);
                    break;
                }
                case 1:  // restart every crashed node
                    for (auto id : c.ids())
                        if (!c.isUp(id)) c.restart(id);
                    break;
                case 2: {  // random partition
                    std::set<kv::raft::NodeId> group;
                    for (auto id : c.ids())
                        if (rng() % 2) group.insert(id);
                    c.partition(group);
                    break;
                }
                case 3:
                    c.heal();
                    break;
                default:
                    if (c.propose("p" + std::to_string(proposed))) ++proposed;
            }
            c.tick();
            if (::testing::Test::HasFailure()) return;
        }

        // Calm down and check that everyone agrees.
        c.setDropRate(0);
        c.heal();
        for (auto id : c.ids())
            if (!c.isUp(id)) c.restart(id);
        c.waitForLeader();
        ASSERT_TRUE(c.propose("final"));
        ASSERT_TRUE(c.tickUntil([&] {
            auto ref = c.applied(c.ids()[0]);
            if (ref.empty() || ref.back() != "final") return false;
            return allApplied(c, ref);
        })) << "nodes did not converge";
    }
}
