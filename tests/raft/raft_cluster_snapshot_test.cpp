// Whole-cluster snapshot scenarios: a bounded log, lagging followers caught up
// by InstallSnapshot, and chaos with snapshots on.

#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test_cluster.h"

using kv::test::TestCluster;

namespace {

std::vector<std::string> commands(int count, const std::string& prefix = "c") {
    std::vector<std::string> out;
    for (int i = 0; i < count; ++i) out.push_back(prefix + std::to_string(i));
    return out;
}

bool allApplied(TestCluster& c, const std::vector<std::string>& expected) {
    for (auto id : c.ids())
        if (c.isUp(id) && c.applied(id) != expected) return false;
    return true;
}

}  // namespace

TEST(RaftClusterSnapshot, SnapshotsKeepTheLogShort) {
    TestCluster::Options o;
    o.snapshotEvery = 10;
    TestCluster c(3, o);
    c.waitForLeader();

    auto cmds = commands(200);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));
    c.tick(20);

    for (auto id : c.ids()) {
        const auto& log = c.node(id).log();
        EXPECT_GT(log.baseIndex(), 150u) << "node " << id << " never compacted";
        EXPECT_LT(log.lastIndex() - log.baseIndex(), 30u) << "node " << id << " still holds a long log";
    }
}

TEST(RaftClusterSnapshot, ALaggingFollowerCatchesUpWithASnapshot) {
    TestCluster::Options o;
    o.snapshotEvery = 8;
    o.snapshotChunkBytes = 64;  // the snapshot is a few hundred bytes: several chunks
    TestCluster c(3, o);
    auto leader = c.waitForLeader();
    const auto lagging = leader % 3 + 1;
    c.isolate(lagging);

    auto cmds = commands(60);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    c.tick(30);
    EXPECT_TRUE(c.applied(lagging).empty());

    // By now the leader has compacted away everything the follower is missing.
    EXPECT_GT(c.node(leader).log().baseIndex(), 40u);

    c.heal();
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));
    EXPECT_GE(c.snapshotsInstalled(), 1u) << "the follower should have been caught up by InstallSnapshot, not by the log";
}

TEST(RaftClusterSnapshot, EveryNodeRestartsFromItsSnapshot) {
    TestCluster::Options o;
    o.snapshotEvery = 7;
    TestCluster c(3, o);
    c.waitForLeader();
    auto cmds = commands(50);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) c.restart(id);  // each rebuilds its state from its snapshot plus the log after it
    c.waitForLeader();
    auto more = commands(5, "after");
    for (const auto& cmd : more) ASSERT_TRUE(c.propose(cmd));
    cmds.insert(cmds.end(), more.begin(), more.end());
    EXPECT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));
}

TEST(RaftClusterSnapshot, ASnapshotOnDemand) {
    TestCluster c(3);  // no automatic snapshots
    c.waitForLeader();
    auto cmds = commands(20);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    ASSERT_TRUE(c.tickUntil([&] { return allApplied(c, cmds); }));

    ASSERT_TRUE(c.snapshot(2));
    EXPECT_EQ(c.node(2).log().baseIndex(), c.node(2).lastApplied());
    EXPECT_FALSE(c.snapshot(2)) << "nothing new to cover";
    c.crash(2);
    c.restart(2);
    EXPECT_EQ(c.applied(2), cmds);
}

// Random crashes, partitions and message loss with snapshots and tiny chunks:
// transfers get interrupted, lost, repeated, and restarted by newer snapshots.
TEST(RaftClusterSnapshot, SafeUnderRandomFailuresWithSnapshots) {
    std::size_t installed = 0;
    for (std::uint64_t seed = 1; seed <= 25; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        TestCluster::Options o;
        o.seed = seed;
        o.snapshotEvery = 6;
        o.snapshotChunkBytes = 48;
        TestCluster c(5, o);
        std::mt19937_64 rng(seed);
        c.setDropRate(0.1);

        int proposed = 0;
        for (int step = 0; step < 400; ++step) {
            switch (rng() % 20) {
                case 0: {
                    auto id = c.ids()[rng() % 5];
                    int up = 0;
                    for (auto n : c.ids()) up += c.isUp(n);
                    if (c.isUp(id) && up > 3) c.crash(id);
                    break;
                }
                case 1:
                    for (auto id : c.ids())
                        if (!c.isUp(id)) c.restart(id);
                    break;
                case 2: {
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

        c.setDropRate(0);
        c.heal();
        for (auto id : c.ids())
            if (!c.isUp(id)) c.restart(id);
        c.waitForLeader();
        ASSERT_TRUE(c.propose("final"));
        ASSERT_TRUE(c.tickUntil(
            [&] {
                auto ref = c.applied(c.ids()[0]);
                if (ref.empty() || ref.back() != "final") return false;
                return allApplied(c, ref);
            },
            1500))
            << "nodes did not converge";
        installed += c.snapshotsInstalled();
    }
    EXPECT_GT(installed, 0u) << "no follower was ever caught up by a snapshot; the chaos did not reach that path";
}
