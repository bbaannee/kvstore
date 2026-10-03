// Tests for the replicated store: Raft + on-disk log + key-value state
// machine, with real files for every node.

#include "server/replicated_node.h"

#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "replicated_cluster.h"

using kv::server::ApplyResult;
using kv::server::RequestStatus;
using kv::storage::LogRecord;
using kv::test::ReplicatedCluster;

namespace {

// True once every running node's map equals `expected`.
bool allNodesHave(ReplicatedCluster& c, const std::map<std::string, std::string>& expected) {
    for (auto id : c.ids()) {
        if (!c.isUp(id)) continue;
        const auto& data = c.node(id).state().data();
        if (data.size() != expected.size()) return false;
        for (const auto& [k, v] : expected) {
            auto it = data.find(k);
            if (it == data.end() || it->second != v) return false;
        }
    }
    return true;
}

}  // namespace

TEST(ReplicatedNode, SingleNodeCommitsAndAnswers) {
    ReplicatedCluster c(1, 1, /*syncOnAppend=*/true);  // exercise the real fsync path once
    c.waitForLeader();

    auto put = c.submitToLeader(LogRecord::put("name", "Bane"));
    ASSERT_TRUE(c.wait(put));
    EXPECT_EQ(put->status, RequestStatus::Committed);

    auto get = c.submitToLeader(LogRecord::get("name"));
    ASSERT_TRUE(c.wait(get));
    EXPECT_EQ(get->status, RequestStatus::Committed);
    EXPECT_EQ(get->result, (ApplyResult{true, "Bane"}));
}

TEST(ReplicatedNode, FollowersRefuseRequests) {
    ReplicatedCluster c(3);
    auto leader = c.waitForLeader();
    for (auto id : c.ids()) {
        if (id == leader) continue;
        auto t = c.submit(id, LogRecord::put("k", "v"));
        ASSERT_TRUE(t->done);  // answered immediately
        EXPECT_EQ(t->status, RequestStatus::NotLeader);
    }
}

TEST(ReplicatedNode, ReadsAndDeletesGoThroughTheLog) {
    ReplicatedCluster c(3);
    c.waitForLeader();

    auto run = [&](const LogRecord& cmd) {
        auto t = c.submitToLeader(cmd);
        EXPECT_TRUE(t && c.wait(t));
        return t->result;
    };
    EXPECT_EQ(run(LogRecord::get("x")), (ApplyResult{false, ""}));
    run(LogRecord::put("x", "1"));
    EXPECT_EQ(run(LogRecord::get("x")), (ApplyResult{true, "1"}));
    EXPECT_EQ(run(LogRecord::del("x")), (ApplyResult{true, ""}));
    EXPECT_EQ(run(LogRecord::del("x")), (ApplyResult{false, ""}));
    EXPECT_EQ(run(LogRecord::get("x")), (ApplyResult{false, ""}));
}

TEST(ReplicatedNode, EveryNodeEndsWithTheSameData) {
    ReplicatedCluster c(5);
    c.waitForLeader();
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 30; ++i) {
        auto key = "k" + std::to_string(i % 10), value = "v" + std::to_string(i);
        auto t = c.submitToLeader(LogRecord::put(key, value));
        ASSERT_TRUE(t && c.wait(t));
        expected[key] = value;
    }
    EXPECT_TRUE(c.tickUntil([&] { return allNodesHave(c, expected); }));
}

// Everything is rebuilt from the Raft log files alone.
TEST(ReplicatedNode, WholeClusterRestartRebuildsTheState) {
    ReplicatedCluster c(3, 1, /*syncOnAppend=*/true);
    c.waitForLeader();
    for (auto [k, v] : std::map<std::string, std::string>{{"a", "1"}, {"b", "2"}, {"c", "3"}}) {
        auto t = c.submitToLeader(LogRecord::put(k, v));
        ASSERT_TRUE(t && c.wait(t));
    }
    c.submitToLeader(LogRecord::del("b"));
    const std::map<std::string, std::string> expected{{"a", "1"}, {"c", "3"}};
    ASSERT_TRUE(c.tickUntil([&] { return allNodesHave(c, expected); }));
    const auto termBefore = c.node(*c.leader()).raft().term();

    for (auto id : c.ids()) c.crash(id);
    for (auto id : c.ids()) {
        c.restart(id);
        EXPECT_GE(c.node(id).raft().term(), termBefore);  // the term survived on disk
        EXPECT_TRUE(c.node(id).state().data().empty());   // memory starts empty...
    }
    c.waitForLeader();
    EXPECT_TRUE(c.tickUntil([&] { return allNodesHave(c, expected); }));  // ...and is rebuilt
}

// A node that votes, crashes and forgets the vote could vote again in the
// same term, letting two candidates both win. The vote must be on disk before
// the "granted" answer leaves the node.
TEST(ReplicatedNode, VoteSurvivesARestart) {
    kv::test::TempDir dir;
    kv::raft::Config cfg;
    cfg.id = 1;
    cfg.peers = {1, 2, 3};

    auto askForVote = [&](kv::server::ReplicatedNode& n, kv::raft::NodeId candidate) {
        n.step(kv::raft::Message{candidate, 1, 5, kv::raft::RequestVote{0, 0}});
        auto out = n.takeOutgoing();
        EXPECT_EQ(out.size(), 1u);
        return std::get<kv::raft::RequestVoteResponse>(out.at(0).body).granted;
    };

    {
        kv::server::ReplicatedNode n(dir.file("node1"), cfg);
        EXPECT_TRUE(askForVote(n, 2));  // votes for node 2 in term 5
    }
    kv::server::ReplicatedNode restarted(dir.file("node1"), cfg);
    EXPECT_EQ(restarted.raft().term(), 5u);
    EXPECT_FALSE(askForVote(restarted, 3));  // already voted in term 5
    EXPECT_TRUE(askForVote(restarted, 2));   // repeating the same vote is fine
}

// The core client guarantee: an entry that was overwritten is never
// reported as committed.
TEST(ReplicatedNode, DeposedLeadersRequestIsAborted) {
    ReplicatedCluster c(3);
    auto oldLeader = c.waitForLeader();
    auto warmup = c.submitToLeader(LogRecord::put("before", "1"));
    ASSERT_TRUE(c.wait(warmup));

    c.isolate(oldLeader);
    auto lost = c.submit(oldLeader, LogRecord::put("lost", "x"));  // accepted, can't replicate
    EXPECT_FALSE(lost->done);

    // The majority elects a new leader and commits its own entry.
    std::optional<ReplicatedCluster::NodeId> newLeader;
    ASSERT_TRUE(c.tickUntil([&] {
        for (auto id : c.ids())
            if (id != oldLeader && c.node(id).raft().role() == kv::raft::Role::Leader) newLeader = id;
        return newLeader.has_value();
    }));
    auto kept = c.submit(*newLeader, LogRecord::put("kept", "y"));
    ASSERT_TRUE(c.wait(kept));
    EXPECT_EQ(kept->status, RequestStatus::Committed);
    EXPECT_FALSE(lost->done);  // still waiting: the old leader hasn't heard about any of this

    // After healing, the old leader finds out. Its entry was overwritten.
    c.heal();
    ASSERT_TRUE(c.wait(lost));
    EXPECT_EQ(lost->status, RequestStatus::Aborted);
    EXPECT_EQ(c.node(oldLeader).pendingRequests(), 0u);

    EXPECT_TRUE(c.tickUntil([&] { return allNodesHave(c, {{"before", "1"}, {"kept", "y"}}); }));
}

// Chaos with real files: crashes, restarts, partitions and message loss.
// Two guarantees are checked against what clients were told:
//   - "Committed" means durable: the write is in every node's final state.
//   - "Aborted" or "NotLeader" means it never happened.
// With snapshotThreshold > 0, nodes also snapshot, compact their log files and
// catch each other up with InstallSnapshot all through the chaos.
static void acknowledgedWritesAreNeverLost(std::size_t snapshotThreshold,
                                            kv::server::ReplicatedNode::Engine engine = kv::server::ReplicatedNode::Engine::Memory) {
    std::size_t totalAborted = 0;
    for (std::uint64_t seed = 1; seed <= 12; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        ReplicatedCluster::Options options;
        options.seed = seed;
        options.snapshotThreshold = snapshotThreshold;
        options.engine = engine;
        ReplicatedCluster c(5, options);
        std::mt19937_64 rng(seed);
        c.setDropRate(0.1);

        struct Write {
            std::string key, value;
            ReplicatedCluster::Ticket ticket;
        };
        std::vector<Write> writes;

        for (int step = 0; step < 250; ++step) {
            switch (rng() % 20) {
                case 0: {  // crash a random node, but keep a majority running
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
                default: {
                    // Unique keys: whatever the final state says about a key,
                    // there is exactly one write that could have put it there.
                    // Mostly to nodes that believe they lead, including stale
                    // leaders cut off in a minority (that's where aborted
                    // writes come from); sometimes to anyone, on purpose.
                    std::vector<kv::raft::NodeId> leaders;
                    for (auto n : c.ids())
                        if (c.isUp(n) && c.node(n).raft().role() == kv::raft::Role::Leader) leaders.push_back(n);
                    auto id = (!leaders.empty() && rng() % 10) ? leaders[rng() % leaders.size()]
                                                                : c.ids()[rng() % 5];
                    if (!c.isUp(id)) break;
                    std::string key = "k" + std::to_string(writes.size());
                    std::string value = "v" + std::to_string(seed) + "_" + std::to_string(writes.size());
                    writes.push_back({key, value, c.submit(id, LogRecord::put(key, value))});
                }
            }
            c.tick();
            if (::testing::Test::HasFailure()) return;
        }

        // Calm down: heal everything and let the cluster converge.
        c.setDropRate(0);
        c.heal();
        for (auto id : c.ids())
            if (!c.isUp(id)) c.restart(id);
        c.waitForLeader();
        auto sentinel = c.submitToLeader(LogRecord::put("final", "x"));
        ASSERT_TRUE(sentinel && c.wait(sentinel));
        ASSERT_TRUE(c.tickUntil([&] {
            const auto& ref = c.node(c.ids()[0]).state().data();
            return ref.count("final") && allNodesHave(c, {ref.begin(), ref.end()});
        })) << "nodes did not converge";

        const auto& finalState = c.node(c.ids()[0]).state();
        std::size_t committed = 0;
        for (const auto& w : writes) {
            if (!w.ticket->done) continue;  // never answered: unknown, may or may not have happened
            auto actual = finalState.get(w.key);
            if (w.ticket->status == RequestStatus::Committed) {
                ++committed;
                EXPECT_EQ(actual, w.value) << "acknowledged write " << w.key << " was lost";
            } else {
                if (w.ticket->status == RequestStatus::Aborted) ++totalAborted;
                EXPECT_EQ(actual, std::nullopt) << w.key << " was reported as not done, but it was applied";
            }
        }
        EXPECT_GT(committed, 0u) << "the test never committed anything; it proves nothing";
    }
    // (With snapshots, a node that is caught up by a snapshot drops requests it can no longer judge instead of
    // aborting them, so this check only makes sense without them.)
    if (snapshotThreshold == 0) {
        EXPECT_GT(totalAborted, 0u) << "no write was ever aborted; the deposed-leader path wasn't exercised";
    }
}

TEST(ReplicatedNode, AcknowledgedWritesAreNeverLost) { acknowledgedWritesAreNeverLost(0); }

TEST(ReplicatedNode, AcknowledgedWritesAreNeverLostWithSnapshots) { acknowledgedWritesAreNeverLost(7); }

// The same, with the LSM engine: crashes now also happen in the middle of its flushes and compactions,
// and a restarted node resumes from the LSM tree's own progress instead of replaying its log.
TEST(ReplicatedNode, AcknowledgedWritesAreNeverLostOnTheLsmEngine) {
    acknowledgedWritesAreNeverLost(0, kv::server::ReplicatedNode::Engine::Lsm);
}

TEST(ReplicatedNode, AcknowledgedWritesAreNeverLostOnTheLsmEngineWithSnapshots) {
    acknowledgedWritesAreNeverLost(7, kv::server::ReplicatedNode::Engine::Lsm);
}
