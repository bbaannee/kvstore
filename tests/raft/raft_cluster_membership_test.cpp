// Whole-cluster membership scenarios: adding and removing servers while the
// cluster keeps working, and chaos with membership changes.

#include <algorithm>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "test_cluster.h"

using kv::raft::ConfigError;
using kv::raft::Member;
using kv::raft::Members;
using kv::raft::NodeId;
using kv::test::TestCluster;

namespace {

TestCluster::Options bootstrapped(std::uint64_t seed = 1) {
    TestCluster::Options o;
    o.seed = seed;
    o.bootstrapConfig = true;  // membership changes need the initial configuration in the log
    return o;
}

Members with(Members m, NodeId id) {
    m.push_back(Member{id, ""});
    return m;
}

Members without(Members m, NodeId id) {
    m.erase(std::remove_if(m.begin(), m.end(), [id](const Member& x) { return x.id == id; }), m.end());
    return m;
}

bool contains(const Members& m, NodeId id) {
    return std::any_of(m.begin(), m.end(), [id](const Member& x) { return x.id == id; });
}

std::vector<std::string> commands(int count, const std::string& prefix = "c") {
    std::vector<std::string> out;
    for (int i = 0; i < count; ++i) out.push_back(prefix + std::to_string(i));
    return out;
}

// True once every one of `nodes` that is running has applied exactly `expected`.
bool applied(TestCluster& c, const std::vector<NodeId>& nodes, const std::vector<std::string>& expected) {
    for (auto id : nodes)
        if (c.isUp(id) && c.applied(id) != expected) return false;
    return true;
}

}  // namespace

TEST(RaftClusterMembership, AddingAServer) {
    TestCluster c(3, bootstrapped());
    c.waitForLeader();
    auto before = commands(5, "before");
    for (const auto& cmd : before) ASSERT_TRUE(c.propose(cmd));

    c.addNode(4);
    EXPECT_FALSE(c.node(4).isMember());
    ASSERT_EQ(c.proposeConfig(with(c.leaderMembers(), 4)), ConfigError::None);

    // The new server learns the whole log, including the old entries, and becomes a member.
    ASSERT_TRUE(c.tickUntil([&] { return c.node(4).isMember() && c.applied(4) == before; }));
    EXPECT_EQ(c.leaderMembers().size(), 4u);

    // And it takes part from then on.
    auto after = commands(5, "after");
    for (const auto& cmd : after) ASSERT_TRUE(c.propose(cmd));
    std::vector<std::string> all = before;
    all.insert(all.end(), after.begin(), after.end());
    EXPECT_TRUE(c.tickUntil([&] { return applied(c, {1, 2, 3, 4}, all); }));
}

TEST(RaftClusterMembership, AServerThatJoinsLateIsCaughtUpWithASnapshot) {
    TestCluster::Options o = bootstrapped();
    o.snapshotEvery = 5;
    o.snapshotChunkBytes = 64;
    TestCluster c(3, o);
    c.waitForLeader();
    auto cmds = commands(40);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    ASSERT_TRUE(c.tickUntil([&] { return applied(c, {1, 2, 3}, cmds); }));

    c.addNode(4);
    ASSERT_EQ(c.proposeConfig(with(c.leaderMembers(), 4)), ConfigError::None);
    EXPECT_TRUE(c.tickUntil([&] { return c.node(4).isMember() && c.applied(4) == cmds; }));
    EXPECT_GE(c.snapshotsInstalled(), 1u) << "the log it needs was compacted, so it must have come as a snapshot";
}

TEST(RaftClusterMembership, OnlyOneChangeAtATime) {
    TestCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    c.tick(10);  // let the leader commit its first entry

    c.addNode(4);
    c.addNode(5);

    // The test network delivers instantly, so a change would commit before we could propose the
    // next one. Cut the leader off first, so that the first change stays uncommitted.
    c.isolate(leader);
    ASSERT_EQ(c.proposeConfig(with(c.leaderMembers(), 4)), ConfigError::None);
    EXPECT_EQ(c.proposeConfig(with(c.leaderMembers(), 5)), ConfigError::Pending)
        << "a second change before the first has committed could create two disjoint majorities";

    // Once it can reach the others, the first commits and the second is fine.
    c.heal();
    ASSERT_TRUE(c.tickUntil([&] { return c.proposeConfig(with(c.leaderMembers(), 5)) == ConfigError::None; }));
    EXPECT_TRUE(c.tickUntil([&] { return c.node(5).isMember(); }));
    EXPECT_EQ(c.leaderMembers().size(), 5u);
}

TEST(RaftClusterMembership, RemovingAFollower) {
    TestCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    const NodeId victim = leader % 3 + 1;
    c.tick(10);

    ASSERT_EQ(c.proposeConfig(without(c.leaderMembers(), victim)), ConfigError::None);
    ASSERT_TRUE(c.tickUntil([&] { return !contains(c.leaderMembers(), victim); }));

    // A 2-server cluster still works (both must be up for a majority of 2).
    auto cmds = commands(10);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    std::vector<NodeId> remaining;
    for (auto id : c.ids())
        if (id != victim) remaining.push_back(id);
    EXPECT_TRUE(c.tickUntil([&] { return applied(c, remaining, cmds); }));
}

TEST(RaftClusterMembership, RemovingTheLeader) {
    TestCluster c(3, bootstrapped());
    auto oldLeader = c.waitForLeader();
    c.tick(10);

    ASSERT_EQ(c.proposeConfig(without(c.leaderMembers(), oldLeader)), ConfigError::None);

    // It steps down once the change has committed, and the others elect a new leader.
    ASSERT_TRUE(c.tickUntil([&] {
        auto l = c.leader();
        return l && *l != oldLeader;
    }));
    EXPECT_FALSE(c.node(oldLeader).isMember());
    EXPECT_NE(c.node(oldLeader).role(), kv::raft::Role::Leader);

    auto cmds = commands(10);
    for (const auto& cmd : cmds) ASSERT_TRUE(c.propose(cmd));
    std::vector<NodeId> remaining;
    for (auto id : c.ids())
        if (id != oldLeader) remaining.push_back(id);
    EXPECT_TRUE(c.tickUntil([&] { return applied(c, remaining, cmds); }));
}

// A removed server never hears that it was removed, so it keeps timing out and
// asking for votes with ever higher terms. That must not disturb the cluster.
TEST(RaftClusterMembership, ARemovedServerCannotDisruptTheCluster) {
    TestCluster c(3, bootstrapped());
    auto leader = c.waitForLeader();
    const NodeId victim = leader % 3 + 1;
    c.tick(10);
    ASSERT_EQ(c.proposeConfig(without(c.leaderMembers(), victim)), ConfigError::None);
    ASSERT_TRUE(c.tickUntil([&] { return !contains(c.leaderMembers(), victim); }));
    c.tick(20);

    const auto term = c.node(leader).term();
    c.tick(400);  // the removed server campaigns again and again
    ASSERT_TRUE(c.leader());
    EXPECT_EQ(*c.leader(), leader) << "a removed server deposed the leader";
    EXPECT_EQ(c.node(leader).term(), term);

    ASSERT_TRUE(c.propose("still works"));
    EXPECT_TRUE(c.tickUntil([&] { return c.applied(leader) == std::vector<std::string>{"still works"}; }));
}

TEST(RaftClusterMembership, AServerThatWasNeverAddedStaysOutOfElections) {
    TestCluster c(3, bootstrapped());
    c.waitForLeader();
    c.addNode(4);  // started, but nobody asked the cluster to add it
    c.tick(400);
    EXPECT_EQ(c.node(4).term(), 0u);
    EXPECT_FALSE(c.node(4).isMember());
    EXPECT_EQ(c.leaderMembers().size(), 3u);
}

// Random membership changes mixed with crashes, partitions and message loss.
// The monitors check Election Safety and State Machine Safety after every step.
TEST(RaftClusterMembership, SafeUnderRandomChangesAndFailures) {
    std::size_t changes = 0;
    for (std::uint64_t seed = 1; seed <= 20; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        TestCluster::Options o = bootstrapped(seed);
        o.snapshotEvery = 8;
        o.snapshotChunkBytes = 64;
        TestCluster c(3, o);
        std::mt19937_64 rng(seed * 31);
        c.setDropRate(0.05);

        NodeId nextId = 4;
        int proposed = 0;
        for (int step = 0; step < 500; ++step) {
            switch (rng() % 25) {
                case 0: {
                    auto id = c.ids()[rng() % c.ids().size()];
                    if (c.isUp(id)) c.crash(id);
                    break;
                }
                case 1:
                    for (auto id : c.ids())
                        if (!c.isUp(id)) c.restart(id);
                    break;
                case 2: {
                    std::set<NodeId> group;
                    for (auto id : c.ids())
                        if (rng() % 2) group.insert(id);
                    c.partition(group);
                    break;
                }
                case 3:
                    c.heal();
                    break;
                case 4:
                case 5: {  // grow or shrink the cluster by one server
                    Members m = c.leaderMembers();
                    if (m.empty()) break;
                    if (rng() % 2 && nextId <= 8) {
                        if (c.proposeConfig(with(m, nextId)) == ConfigError::None) {
                            c.addNode(nextId++);
                            ++changes;
                        }
                    } else if (m.size() > 3) {
                        if (c.proposeConfig(without(m, m[rng() % m.size()].id)) == ConfigError::None) ++changes;
                    }
                    break;
                }
                default:
                    if (c.propose("p" + std::to_string(proposed))) ++proposed;
            }
            c.tick();
            if (::testing::Test::HasFailure()) return;
        }

        // Calm down. Whoever the members are now must agree on everything.
        c.setDropRate(0);
        c.heal();
        for (auto id : c.ids())
            if (!c.isUp(id)) c.restart(id);
        c.waitForLeader();
        ASSERT_TRUE(c.propose("final"));
        ASSERT_TRUE(c.tickUntil(
            [&] {
                auto leader = c.leader();
                if (!leader) return false;
                auto ref = c.applied(*leader);
                if (ref.empty() || ref.back() != "final") return false;
                std::vector<NodeId> members;
                for (const auto& m : c.node(*leader).members()) members.push_back(m.id);
                return applied(c, members, ref);
            },
            3000))
            << "the members did not converge";
    }
    EXPECT_GT(changes, 20u) << "too few configuration changes went through for the test to mean much";
}
