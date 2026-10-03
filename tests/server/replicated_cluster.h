#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "server/replicated_node.h"
#include "temp_dir.h"

namespace kv::test {

// A cluster of real ReplicatedNodes, each with its own data directory on
// disk, connected by a fake network. Like TestCluster (tests/raft), all
// randomness comes from one seed, so a failing run can be reproduced.
//
// The difference: here the nodes really write their Raft log and snapshots to
// files and really rebuild from them after a crash, so this exercises storage,
// recovery and the client-request path, not just the protocol.
class ReplicatedCluster {
public:
    using NodeId = raft::NodeId;

    // What a client eventually learns about a request.
    struct Outcome {
        bool done = false;
        server::RequestStatus status = server::RequestStatus::Aborted;
        server::ApplyResult result;
    };
    using Ticket = std::shared_ptr<Outcome>;

    struct Options {
        std::uint64_t seed = 1;

        // false skips the real fsync: a crash in these tests loses the process,
        // not the OS page cache, so it changes nothing about what we can
        // observe, and it keeps the chaos tests fast.
        bool syncOnAppend = false;

        std::size_t snapshotThreshold = 0;  // 0: never snapshot
        bool bootstrapConfig = false;       // needed for membership changes

        // The LSM engine, with tiny limits so that even a short test flushes
        // and compacts many times.
        server::ReplicatedNode::Engine engine = server::ReplicatedNode::Engine::Memory;
    };

    explicit ReplicatedCluster(std::size_t size, std::uint64_t seed = 1, bool syncOnAppend = false)
        : ReplicatedCluster(size, makeOptions(seed, syncOnAppend)) {}

    ReplicatedCluster(std::size_t size, Options options) : options_(options), rng_(options.seed) {
        for (NodeId id = 1; id <= size; ++id) initial_.push_back(id);
        ids_ = initial_;
        for (NodeId id : ids_) start(id);
    }

    const std::vector<NodeId>& ids() const { return ids_; }
    server::ReplicatedNode& node(NodeId id) { return *nodes_.at(id); }
    bool isUp(NodeId id) const { return nodes_.at(id) != nullptr; }

    // One unit of time: every running node ticks, then messages are
    // delivered in random order until the network is quiet.
    void tick() {
        for (NodeId id : ids_)
            if (isUp(id)) {
                node(id).tick();
                collect(id);
            }
        deliverAll();
    }
    void tick(int n) {
        for (int i = 0; i < n; ++i) tick();
    }

    bool tickUntil(const std::function<bool()>& done, int maxTicks = 500) {
        for (int i = 0; i < maxTicks; ++i) {
            if (done()) return true;
            tick();
        }
        return done();
    }

    // The running leader with the highest term, if any.
    std::optional<NodeId> leader() {
        std::optional<NodeId> best;
        for (NodeId id : ids_)
            if (isUp(id) && node(id).raft().role() == raft::Role::Leader &&
                (!best || node(id).raft().term() > node(*best).raft().term()))
                best = id;
        return best;
    }

    NodeId waitForLeader() {
        std::optional<NodeId> l;
        EXPECT_TRUE(tickUntil([&] { return (l = leader()).has_value(); })) << "no leader elected";
        return l.value_or(0);
    }

    // Submits a command (through the Raft log) to a specific node.
    Ticket submit(NodeId id, const storage::LogRecord& cmd) { return submit(id, server::Command{0, 0, cmd}); }

    // The same, with a client identity (so the cluster can recognize retries).
    Ticket submit(NodeId id, const server::Command& cmd) {
        auto ticket = std::make_shared<Outcome>();
        node(id).submit(cmd, [ticket](server::RequestStatus s, const server::ApplyResult& r) {
            *ticket = Outcome{true, s, r};
        });
        collect(id);
        deliverAll();
        return ticket;
    }

    // Submits to the current leader; null if there is none.
    Ticket submitToLeader(const storage::LogRecord& cmd) {
        auto l = leader();
        return l ? submit(*l, cmd) : nullptr;
    }

    // A linearizable read through ReadIndex (no log entry).
    Ticket read(NodeId id, const std::string& key) {
        auto ticket = std::make_shared<Outcome>();
        node(id).read(key, [ticket](server::RequestStatus s, const server::ApplyResult& r) {
            *ticket = Outcome{true, s, r};
        });
        collect(id);
        deliverAll();
        return ticket;
    }

    // Asks a node (normally the leader) to change the configuration.
    Ticket changeMembership(NodeId id, raft::Members members) {
        auto ticket = std::make_shared<Outcome>();
        node(id).changeMembership(std::move(members), [ticket](server::RequestStatus s, const server::ApplyResult& r) {
            *ticket = Outcome{true, s, r};
        });
        collect(id);
        deliverAll();
        return ticket;
    }

    // Ticks until the request is answered.
    bool wait(const Ticket& t, int maxTicks = 500) {
        return tickUntil([&] { return t->done; }, maxTicks);
    }

    // Starts a new server that isn't a member yet.
    void addNode(NodeId id) {
        ids_.push_back(id);
        joining_.insert(id);
        start(id);
    }

    // --- Failures ---------------------------------------------------------

    // The process dies: memory is gone, the files stay.
    void crash(NodeId id) { nodes_[id].reset(); }

    // Starts a new process that recovers from the files.
    void restart(NodeId id) { start(id); }

    void partition(const std::set<NodeId>& group) {
        cut_.clear();
        for (NodeId a : ids_)
            for (NodeId b : ids_)
                if (group.count(a) != group.count(b)) cut_.insert({a, b});
    }
    void isolate(NodeId id) { partition({id}); }
    void heal() { cut_.clear(); }
    void setDropRate(double rate) { dropRate_ = rate; }

    std::string dataDir(NodeId id) { return dir_.file("node" + std::to_string(id)); }

private:
    static Options makeOptions(std::uint64_t seed, bool syncOnAppend) {
        Options o;
        o.seed = seed;
        o.syncOnAppend = syncOnAppend;
        return o;
    }

    void start(NodeId id) {
        raft::Config cfg;
        cfg.id = id;
        cfg.peers = initial_;
        cfg.bootstrapConfigEntry = options_.bootstrapConfig;
        cfg.joining = joining_.count(id) > 0;
        cfg.randomSeed = options_.seed * 1000 + id * 10 + starts_[id]++;

        server::ReplicatedNode::Options o;
        o.syncOnAppend = options_.syncOnAppend;
        o.snapshotThreshold = options_.snapshotThreshold;
        o.engine = options_.engine;
        o.lsm.memtableBytes = 2000;  // tiny, so that tests flush and compact a lot
        o.lsm.blockBytes = 256;
        o.lsm.l0CompactionTrigger = 3;
        o.lsm.level1MaxBytes = 6000;
        o.lsm.levelMultiplier = 3;
        o.lsm.targetFileBytes = 1500;
        nodes_[id] = std::make_unique<server::ReplicatedNode>(dataDir(id), cfg, o);
        collect(id);
    }

    void collect(NodeId id) {
        for (auto& m : node(id).takeOutgoing()) inflight_.push_back(std::move(m));

        // Election Safety: at most one leader per term. A node that forgot
        // its vote after a restart could make this fail.
        const raft::RaftNode& r = node(id).raft();
        if (r.role() == raft::Role::Leader) {
            auto [it, inserted] = leaderOfTerm_.emplace(r.term(), id);
            EXPECT_EQ(it->second, id) << "two leaders in term " << r.term();
        }
    }

    void deliverAll() {
        int budget = 100000;  // guards against a livelock bug hanging the test
        while (!inflight_.empty() && budget-- > 0) {
            std::size_t pick = rng_() % inflight_.size();
            raft::Message m = std::move(inflight_[pick]);
            inflight_.erase(inflight_.begin() + static_cast<std::ptrdiff_t>(pick));

            if (std::find(ids_.begin(), ids_.end(), m.to) == ids_.end()) continue;
            if (!isUp(m.to) || cut_.count({m.from, m.to})) continue;
            if (dropRate_ > 0 && std::uniform_real_distribution<>(0, 1)(rng_) < dropRate_) continue;

            node(m.to).step(m);
            collect(m.to);
        }
        ASSERT_GT(budget, 0) << "message storm: the network never went quiet";
    }

    TempDir dir_;
    Options options_;
    std::vector<NodeId> initial_;
    std::vector<NodeId> ids_;
    std::set<NodeId> joining_;
    std::map<NodeId, std::unique_ptr<server::ReplicatedNode>> nodes_;
    std::map<NodeId, int> starts_;
    std::map<raft::Term, NodeId> leaderOfTerm_;
    std::deque<raft::Message> inflight_;
    std::set<std::pair<NodeId, NodeId>> cut_;
    std::mt19937_64 rng_;
    double dropRate_ = 0;
};

}  // namespace kv::test
