// Does the replicated store actually behave like ONE machine? These tests
// record what clients saw while the cluster was being abused, and let the
// linearizability checker decide.

#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "replicated_cluster.h"
#include "verify/history.h"
#include "verify/linearizability.h"

using kv::storage::LogRecord;
using kv::test::ReplicatedCluster;
using kv::verify::History;
using kv::verify::Operation;
using kv::server::RequestStatus;

namespace {

// A simulated client: it has at most one operation in flight, and gives up
// waiting after a while (a timeout), leaving that operation's outcome unknown.
struct SimClient {
    int id = 0;
    std::optional<std::size_t> historyId;  // the operation in flight
    ReplicatedCluster::Ticket ticket;
    int waited = 0;
};

}  // namespace

// Random operations from several clients while nodes crash and restart, the
// network partitions and messages are lost. Whatever the clients were told
// must be explainable by a single sequential order.
//
// useReadIndex: reads take the fast path (ReadIndex) instead of going through the log.
// snapshotThreshold: nodes snapshot, compact and catch each other up with snapshots.
static void historiesUnderChaosAreLinearizable(
    bool useReadIndex, std::size_t snapshotThreshold, std::uint64_t seeds,
    kv::server::ReplicatedNode::Engine engine = kv::server::ReplicatedNode::Engine::Memory) {
    std::size_t totalCompleted = 0, totalPending = 0;

    for (std::uint64_t seed = 1; seed <= seeds; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        ReplicatedCluster::Options options;
        options.seed = seed;
        options.snapshotThreshold = snapshotThreshold;
        options.engine = engine;
        ReplicatedCluster c(5, options);
        std::mt19937_64 rng(seed * 7919);
        c.setDropRate(0.1);

        History history;
        std::vector<SimClient> clients(4);
        for (int i = 0; i < 4; ++i) clients[i].id = i;
        int nextValue = 0;

        auto finish = [&](SimClient& cl) {
            if (!cl.ticket->done) return false;
            switch (cl.ticket->status) {
                case RequestStatus::Committed: {
                    const auto& r = cl.ticket->result;
                    history.complete(*cl.historyId, r.found, r.value);
                    break;
                }
                case RequestStatus::NotLeader:  // refused up front: it never happened
                case RequestStatus::Aborted:    // our entry was overwritten: it never happened
                case RequestStatus::Rejected:
                    history.cancel(*cl.historyId);
                    break;
            }
            return true;
        };

        for (int step = 0; step < 300; ++step) {
            // Chaos.
            switch (rng() % 25) {
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
                    break;
            }

            // Clients.
            for (SimClient& cl : clients) {
                if (cl.historyId) {
                    if (finish(cl) || ++cl.waited > 60) cl.historyId.reset();  // answered, or timed out
                    continue;
                }
                if (rng() % 3) continue;  // think for a while

                const std::string key(1, static_cast<char>('a' + rng() % 3));
                std::vector<kv::raft::NodeId> leaders;  // include stale leaders: that's where bugs hide
                for (auto n : c.ids())
                    if (c.isUp(n) && c.node(n).raft().role() == kv::raft::Role::Leader) leaders.push_back(n);
                auto target = (!leaders.empty() && rng() % 10) ? leaders[rng() % leaders.size()] : c.ids()[rng() % 5];
                if (!c.isUp(target)) continue;

                Operation::Type type;
                LogRecord op;
                std::string input;
                switch (rng() % 3) {
                    case 0:
                        type = Operation::Type::Put;
                        input = "v" + std::to_string(nextValue++);
                        op = LogRecord::put(key, input);
                        break;
                    case 1:
                        type = Operation::Type::Get;
                        op = LogRecord::get(key);
                        break;
                    default:
                        type = Operation::Type::Del;
                        op = LogRecord::del(key);
                }
                cl.historyId = history.begin(cl.id, type, key, input);
                cl.ticket = (useReadIndex && type == Operation::Type::Get) ? c.read(target, key) : c.submit(target, op);
                cl.waited = 0;
                if (finish(cl)) cl.historyId.reset();  // refused immediately
            }
            c.tick();
            if (::testing::Test::HasFailure()) return;
        }

        // Clients still waiting at the end stay unanswered: pending.
        auto ops = history.operations();
        for (const Operation& op : ops) (op.completed ? totalCompleted : totalPending)++;

        auto result = kv::verify::checkLinearizable(ops);
        ASSERT_FALSE(result.inconclusive) << result.explanation;
        ASSERT_TRUE(result.linearizable) << result.explanation;
    }
    EXPECT_GT(totalCompleted, 500u) << "too few operations completed for the test to mean much";
    EXPECT_GT(totalPending, 0u) << "no operation was left unanswered; the unknown-outcome path wasn't exercised";
}

TEST(LinearizabilityCluster, HistoriesUnderChaosAreLinearizable) { historiesUnderChaosAreLinearizable(false, 0, 12); }

TEST(LinearizabilityCluster, HistoriesWithReadIndexReadsAreLinearizable) {
    historiesUnderChaosAreLinearizable(true, 0, 25);
}

TEST(LinearizabilityCluster, HistoriesWithReadIndexAndSnapshotsAreLinearizable) {
    historiesUnderChaosAreLinearizable(true, 6, 25);
}

TEST(LinearizabilityCluster, HistoriesOnTheLsmEngineAreLinearizable) {
    historiesUnderChaosAreLinearizable(true, 6, 25, kv::server::ReplicatedNode::Engine::Lsm);
}

// The safe way to read fast. The same scenario as below, but with the real
// read path: the cut-off old leader can't confirm that it still leads, so it
// never answers (and then fails the read), instead of returning old data.
TEST(LinearizabilityCluster, ReadIndexNeverAnswersFromADeposedLeader) {
    ReplicatedCluster c(3);
    History history;

    auto oldLeader = c.waitForLeader();
    auto put1 = c.submitToLeader(LogRecord::put("x", "1"));
    ASSERT_TRUE(c.wait(put1));

    c.isolate(oldLeader);
    ASSERT_TRUE(c.tickUntil([&] {
        for (auto id : c.ids())
            if (id != oldLeader && c.node(id).raft().role() == kv::raft::Role::Leader) return true;
        return false;
    }));
    auto put2 = c.submitToLeader(LogRecord::put("x", "2"));
    ASSERT_TRUE(c.wait(put2));

    // A read sent to the old leader, which still believes it leads.
    ASSERT_EQ(c.node(oldLeader).raft().role(), kv::raft::Role::Leader);
    auto stale = c.read(oldLeader, "x");
    EXPECT_FALSE(stale->done) << "answered without confirming it is still the leader";

    // It steps down once it notices it can't reach a majority, and the read fails (it was never served).
    ASSERT_TRUE(c.wait(stale, 100));
    EXPECT_EQ(stale->status, RequestStatus::NotLeader);

    // The same read on the real leader sees the new value.
    auto fresh = c.read(*c.leader(), "x");
    ASSERT_TRUE(c.wait(fresh));
    EXPECT_EQ(fresh->status, RequestStatus::Committed);
    EXPECT_EQ(fresh->result.value, "2");
}

// What a stale read looks like to the checker. Serving a read from a node's
// local state is tempting (it's fast) but wrong: a node cut off from the
// cluster still thinks it's the leader and answers with old data.
TEST(LinearizabilityCluster, CheckerCatchesStaleReadsFromADeposedLeader) {
    ReplicatedCluster c(3);
    History history;

    auto run = [&](int client, const LogRecord& op, Operation::Type type, const std::string& key,
                   const std::string& input, std::optional<ReplicatedCluster::NodeId> node = std::nullopt) {
        auto id = history.begin(client, type, key, input);
        auto ticket = node ? c.submit(*node, op) : c.submitToLeader(op);
        EXPECT_TRUE(ticket && c.wait(ticket));
        history.complete(id, ticket->result.found, ticket->result.value);
    };

    auto oldLeader = c.waitForLeader();
    run(1, LogRecord::put("x", "1"), Operation::Type::Put, "x", "1");

    // The old leader is cut off; the rest elect a new one, which writes x=2.
    c.isolate(oldLeader);
    ASSERT_TRUE(c.tickUntil([&] {
        for (auto id : c.ids())
            if (id != oldLeader && c.node(id).raft().role() == kv::raft::Role::Leader) return true;
        return false;
    }));
    run(2, LogRecord::put("x", "2"), Operation::Type::Put, "x", "2");

    // A "fast" read served from the old leader's own state: it still holds x=1.
    auto stale = history.begin(3, Operation::Type::Get, "x");
    auto local = c.node(oldLeader).state().get("x");
    ASSERT_EQ(local, "1");
    history.complete(stale, local.has_value(), local.value_or(""));

    auto result = kv::verify::checkLinearizable(history.operations());
    EXPECT_FALSE(result.linearizable) << "the stale read went unnoticed";
    EXPECT_NE(result.explanation.find("get(x) -> 1"), std::string::npos) << result.explanation;
}
