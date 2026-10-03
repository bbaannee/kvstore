// End-to-end tests of the newer features over real TCP: adding and removing
// servers, snapshots, the read and commit options, and a linearizability check
// of what concurrent clients saw while servers were being killed.

#include <atomic>
#include <chrono>
#include <map>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/client.h"
#include "live_cluster.h"
#include "verify/history.h"
#include "verify/linearizability.h"

using kv::client::Client;
using kv::test::LiveCluster;
using namespace std::chrono_literals;

namespace {

Client makeClient(const std::vector<std::string>& servers, std::chrono::milliseconds total = 15s) {
    Client::Options o;
    o.servers = servers;
    o.totalTimeout = total;
    return Client(o);
}

std::map<std::string, std::string> contents(const kv::server::ReplicatedNode& node) {
    const auto& data = node.state().data();
    return {data.begin(), data.end()};
}

// A server that was added learns the leader, the other members and the whole log by itself.
void addServer(LiveCluster& cluster, Client& admin, kv::raft::NodeId id) {
    cluster.addNode(id);
    auto r = admin.addServer(id, cluster.address(id).toString());
    ASSERT_TRUE(r.ok) << r.error;
}

}  // namespace

TEST(NodeServerCluster, AServerCanBeAddedToARunningCluster) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 20; ++i) {
        expected["before" + std::to_string(i)] = "v" + std::to_string(i);
        ASSERT_TRUE(client.put("before" + std::to_string(i), "v" + std::to_string(i)).ok);
    }

    addServer(cluster, client, 4);

    for (int i = 0; i < 10; ++i) {
        expected["after" + std::to_string(i)] = "w" + std::to_string(i);
        ASSERT_TRUE(client.put("after" + std::to_string(i), "w" + std::to_string(i)).ok);
    }
    std::this_thread::sleep_for(600ms);  // let the followers apply the last entries
    cluster.stopAll();

    // The new server holds everything: the old entries (replicated to it from the log) and the new ones.
    for (kv::raft::NodeId id : {1, 2, 3, 4}) EXPECT_EQ(contents(cluster.node(id)), expected) << "node " << id;
    EXPECT_EQ(cluster.node(4).raft().members().size(), 4u);
    EXPECT_TRUE(cluster.node(4).raft().isMember());
}

TEST(NodeServerCluster, ANewServerCanBeUsedByClientsAndHelpsKeepTheClusterAvailable) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());
    ASSERT_TRUE(client.put("k", "1").ok);
    addServer(cluster, client, 4);
    addServer(cluster, client, 5);  // a 5-server cluster survives two failures

    cluster.stop(1);
    cluster.stop(2);
    auto r = client.put("k", "2");
    ASSERT_TRUE(r.ok) << "3 of 5 servers are up: " << r.error;
    EXPECT_EQ(client.get("k").value, "2");
}

TEST(NodeServerCluster, AServerCanBeRemoved) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());
    ASSERT_TRUE(client.put("k", "1").ok);

    auto r = client.removeServer(3);
    ASSERT_TRUE(r.ok) << r.error;

    // Still works with the 2 remaining servers; the removed one takes no part.
    cluster.stop(3);
    ASSERT_TRUE(client.put("k", "2").ok);
    EXPECT_EQ(client.get("k").value, "2");

    // Removing it again, or something that was never there, is rejected.
    EXPECT_FALSE(client.removeServer(3).ok);
    EXPECT_FALSE(client.removeServer(99).ok);
    EXPECT_FALSE(client.addServer(1, "127.0.0.1:1").ok) << "already a member";
    EXPECT_FALSE(client.addServer(7, "not an address").ok);
}

TEST(NodeServerCluster, ALateServerIsCaughtUpWithASnapshot) {
    LiveCluster::Options o;
    o.snapshotThreshold = 10;
    LiveCluster cluster(3, o);
    Client client = makeClient(cluster.serverList());
    std::map<std::string, std::string> expected;
    for (int i = 0; i < 100; ++i) {
        expected["key" + std::to_string(i % 15)] = "value" + std::to_string(i);
        ASSERT_TRUE(client.put("key" + std::to_string(i % 15), "value" + std::to_string(i)).ok);
    }

    addServer(cluster, client, 4);
    ASSERT_TRUE(client.put("marker", "x").ok);
    expected["marker"] = "x";
    std::this_thread::sleep_for(800ms);
    cluster.stopAll();

    EXPECT_EQ(contents(cluster.node(4)), expected);
    EXPECT_GT(cluster.node(4).raft().snapshotIndex(), 50u) << "the log it needed was compacted, so it came as a snapshot";
}

TEST(NodeServerCluster, ServersRestartFromTheirSnapshots) {
    LiveCluster::Options o;
    o.snapshotThreshold = 8;
    LiveCluster cluster(3, o);
    {
        Client client = makeClient(cluster.serverList());
        for (int i = 0; i < 60; ++i) ASSERT_TRUE(client.put("k" + std::to_string(i % 10), std::to_string(i)).ok);
    }
    for (kv::raft::NodeId id : {1, 2, 3}) cluster.restart(id);

    Client client = makeClient(cluster.serverList());
    for (int i = 50; i < 60; ++i) EXPECT_EQ(client.get("k" + std::to_string(i % 10)).value, std::to_string(i));
}

TEST(NodeServerCluster, WorksWithTheSlowPathsToo) {
    // Reads through the log, and one fsync-free commit per request, must behave the same.
    LiveCluster::Options o;
    o.readIndex = false;
    o.groupCommit = false;
    LiveCluster cluster(3, o);
    Client client = makeClient(cluster.serverList());
    ASSERT_TRUE(client.put("a", "1").ok);
    EXPECT_EQ(client.get("a").value, "1");
    ASSERT_TRUE(client.del("a").ok);
    EXPECT_FALSE(client.get("a").found);
}

// Concurrent clients, random operations, servers being killed and restarted:
// whatever the clients saw (including operations that timed out) must be
// explainable by ONE sequential order. Everything here is real: sockets,
// threads, restarts from disk, retries with deduplication, ReadIndex reads.
static void historiesOverRealSocketsAreLinearizable(LiveCluster::Options o) {
    LiveCluster cluster(3, o);
    kv::verify::History history;
    std::atomic<bool> stop{false};

    auto worker = [&](int clientNo) {
        Client::Options co;
        co.servers = cluster.serverList();
        // Short timeouts, so that during a failover some operations give up: those end with an
        // unknown outcome (they may or may not have happened), which the checker must handle.
        co.attemptTimeout = 150ms;
        co.totalTimeout = 400ms;
        Client client(co);
        std::mt19937_64 rng(clientNo * 977 + 5);
        int n = 0;
        while (!stop) {
            using kv::verify::Operation;
            const std::string key(1, static_cast<char>('a' + rng() % 3));
            switch (rng() % 3) {
                case 0: {
                    const std::string value = std::to_string(clientNo) + "-" + std::to_string(n++);
                    auto id = history.begin(clientNo, Operation::Type::Put, key, value);
                    if (client.put(key, value).ok) history.complete(id, true);  // no answer: its outcome stays unknown
                    break;
                }
                case 1: {
                    auto id = history.begin(clientNo, Operation::Type::Get, key);
                    auto r = client.get(key);
                    if (r.ok) history.complete(id, r.found, r.value);
                    break;
                }
                default: {
                    auto id = history.begin(clientNo, Operation::Type::Del, key);
                    auto r = client.del(key);
                    if (r.ok) history.complete(id, r.found);
                }
            }
        }
    };

    std::vector<std::thread> workers;
    for (int c = 0; c < 4; ++c) workers.emplace_back(worker, c);

    // Meanwhile: kill each server in turn (the leader among them), let it come back from its disk.
    for (kv::raft::NodeId victim : {1, 2, 3, 1}) {
        std::this_thread::sleep_for(400ms);
        cluster.stop(victim);
        std::this_thread::sleep_for(500ms);
        cluster.restart(victim);
    }
    std::this_thread::sleep_for(500ms);
    stop = true;
    for (auto& t : workers) t.join();
    cluster.stopAll();

    auto operations = history.operations();
    std::size_t completed = 0, unknown = 0;
    for (const auto& op : operations) (op.completed ? completed : unknown)++;
    EXPECT_GT(completed, 40u) << "too few operations went through for the test to mean much";

    auto result = kv::verify::checkLinearizable(operations);
    EXPECT_FALSE(result.inconclusive) << result.explanation;
    EXPECT_TRUE(result.linearizable) << result.explanation;
    std::cerr << "[ info ] " << completed << " operations completed, " << unknown << " of unknown outcome, "
              << result.statesExplored << " states explored\n";
}

TEST(NodeServerCluster, HistoriesOverRealSocketsWithServerRestartsAreLinearizable) {
    LiveCluster::Options o;
    o.snapshotThreshold = 12;  // snapshots in the mix too
    historiesOverRealSocketsAreLinearizable(o);
}

TEST(NodeServerCluster, HistoriesOnTheLsmEngineOverRealSocketsAreLinearizable) {
    LiveCluster::Options o;
    o.snapshotThreshold = 12;
    o.engine = kv::server::ReplicatedNode::Engine::Lsm;
    historiesOverRealSocketsAreLinearizable(o);
}

// A whole cluster of servers whose data lives in LSM trees on disk: restarted, it serves what it had.
TEST(NodeServerCluster, ServersOnTheLsmEngineKeepTheirDataAcrossRestarts) {
    LiveCluster::Options o;
    o.engine = kv::server::ReplicatedNode::Engine::Lsm;
    o.snapshotThreshold = 25;
    LiveCluster cluster(3, o);
    std::map<std::string, std::string> expected;
    {
        Client client = makeClient(cluster.serverList());
        for (int i = 0; i < 300; ++i) {
            const std::string k = "key" + std::to_string(i % 60), v = "value" + std::to_string(i);
            ASSERT_TRUE(client.put(k, v).ok);
            expected[k] = v;
        }
        ASSERT_TRUE(client.del("key7").ok);
        expected.erase("key7");
    }
    for (kv::raft::NodeId id : {1, 2, 3}) cluster.restart(id);

    Client client = makeClient(cluster.serverList());
    for (const auto& [k, v] : expected) ASSERT_EQ(client.get(k).value, v) << k;
    EXPECT_FALSE(client.get("key7").found);

    std::this_thread::sleep_for(500ms);
    cluster.stopAll();
    for (kv::raft::NodeId id : {1, 2, 3}) EXPECT_EQ(contents(cluster.node(id)), expected) << "node " << id;
}
