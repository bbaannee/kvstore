#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "net/socket.h"
#include "server/node_server.h"
#include "temp_dir.h"

namespace kv::test {

// A real cluster inside the test process: every node is a NodeServer with
// its own data directory, its own thread running its event loop, and its own
// listening TCP port on localhost. Nothing is faked except that they share a
// process; all communication goes through real sockets.
class LiveCluster {
public:
    using NodeId = raft::NodeId;

    struct Options {
        // sendBufferBytes > 0 shrinks every socket's send buffer, so big messages
        // can only be written a few KB at a time (the "partial write" path).
        int sendBufferBytes = 0;

        // The speed of time. Election timeouts are 10-20 ticks, so the default
        // 10 ms means 100-200 ms. Raft only works if sending a message takes
        // much less time than the election timeout: a heartbeat stuck behind a
        // big message looks like a dead leader. Tests that slow the network down
        // on purpose must slow the clock down too.
        int tickMs = 10;

        std::size_t snapshotThreshold = 0;
        bool readIndex = true;
        bool groupCommit = true;
        server::ReplicatedNode::Engine engine = server::ReplicatedNode::Engine::Memory;
    };

    explicit LiveCluster(std::size_t size, int sendBufferBytes = 0, int tickMs = 10)
        : LiveCluster(size, makeOptions(sendBufferBytes, tickMs)) {}

    LiveCluster(std::size_t size, Options options) : options_(options) {
        for (NodeId id = 1; id <= size; ++id) ids_.push_back(id);

        // Create every server first: binding port 0 is how we learn the ports...
        for (NodeId id : ids_) {
            servers_[id] = makeServer(id, /*port=*/0);
            addresses_[id] = {"127.0.0.1", servers_[id]->port()};
        }
        initial_ = addresses_;
        // ...then tell everyone where everyone is, and start them.
        for (NodeId id : ids_) startThread(id);
    }

    ~LiveCluster() { stopAll(); }

    const std::vector<NodeId>& ids() const { return ids_; }

    std::vector<std::string> serverList() const {
        std::vector<std::string> out;
        for (const auto& [id, addr] : addresses_) out.push_back(addr.toString());
        return out;
    }
    std::vector<net::Address> addressList() const {
        std::vector<net::Address> out;
        for (const auto& [id, addr] : addresses_) out.push_back(addr);
        return out;
    }
    net::Address address(NodeId id) const { return addresses_.at(id); }

    // Starts a new server that joins the cluster later. It is running but NOT
    // a member until a client asks the cluster to add it (Client::addServer).
    void addNode(NodeId id) {
        ids_.push_back(id);
        joining_.insert(id);
        servers_[id] = makeServer(id, /*port=*/0, /*join=*/true);
        addresses_[id] = {"127.0.0.1", servers_[id]->port()};
        startThread(id);
    }

    // Stops a node's event loop. The process-level equivalent is kill: its
    // data directory stays, and nothing is flushed or announced.
    void stop(NodeId id) {
        if (!threads_.count(id)) return;
        servers_[id]->stop();
        threads_[id].join();
        threads_.erase(id);
    }

    // Starts a new server on the same port with the same data directory, so
    // it recovers from disk and the other nodes reconnect to it.
    void restart(NodeId id) {
        stop(id);
        servers_[id].reset();  // closes the old listening socket
        servers_[id] = makeServer(id, addresses_[id].port, joining_.count(id) > 0);
        startThread(id);
    }

    void stopAll() {
        for (NodeId id : ids_) stop(id);
    }

    // Only safe while the node is stopped.
    const server::ReplicatedNode& node(NodeId id) const {
        EXPECT_EQ(threads_.count(id), 0u) << "node " << id << " is still running";
        return servers_.at(id)->node();
    }

private:
    static Options makeOptions(int sendBufferBytes, int tickMs) {
        Options o;
        o.sendBufferBytes = sendBufferBytes;
        o.tickMs = tickMs;
        return o;
    }

    std::unique_ptr<server::NodeServer> makeServer(NodeId id, std::uint16_t port, bool join = false) {
        server::NodeServer::Options o;
        o.id = id;
        o.dataDir = dir_.file("node" + std::to_string(id));
        o.listen = {"127.0.0.1", port};
        o.tickInterval = std::chrono::milliseconds(options_.tickMs);
        o.join = join;
        o.snapshotThreshold = options_.snapshotThreshold;
        o.readIndex = options_.readIndex;
        o.groupCommit = options_.groupCommit;
        o.engine = options_.engine;
        o.lsm.memtableBytes = 4000;  // tiny, so that tests flush and compact a lot
        o.lsm.blockBytes = 256;
        o.lsm.l0CompactionTrigger = 3;
        o.lsm.level1MaxBytes = 12000;
        o.lsm.levelMultiplier = 3;
        o.lsm.targetFileBytes = 3000;
        o.syncOnAppend = false;  // the disk format isn't what's tested here
        o.sendBufferBytes = options_.sendBufferBytes;
        return std::make_unique<server::NodeServer>(o);
    }

    void startThread(NodeId id) {
        // The initial cluster is recorded as the first configuration; a joining server passes nothing.
        servers_[id]->start(joining_.count(id) ? std::map<NodeId, net::Address>{} : initial_);
        server::NodeServer* s = servers_[id].get();
        threads_[id] = std::thread([s] { s->run(); });
    }

    TempDir dir_;
    Options options_;
    std::vector<NodeId> ids_;
    std::set<NodeId> joining_;
    std::map<NodeId, std::unique_ptr<server::NodeServer>> servers_;
    std::map<NodeId, net::Address> addresses_;
    std::map<NodeId, net::Address> initial_;
    std::map<NodeId, std::thread> threads_;
};

}  // namespace kv::test
