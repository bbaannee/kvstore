#pragma once

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

#include "raft/raft_node.h"

namespace kv::test {

// A whole Raft cluster in one process, with a fake network and fake disks.
//
// Everything is deterministic for a given seed: message delivery order,
// message loss, and every node's election timeouts. If a test fails, the
// same seed reproduces it exactly.
//
// While it runs, the cluster checks Raft's safety guarantees after every
// step and fails the test the moment one is violated:
//   - Election Safety: at most one leader per term.
//   - State Machine Safety: no two nodes apply different entries at the
//     same index.
class TestCluster {
public:
    using NodeId = raft::NodeId;

    explicit TestCluster(std::size_t size, std::uint64_t seed = 1) : rng_(seed) {
        for (NodeId id = 1; id <= size; ++id) ids_.push_back(id);
        seed_ = seed;
        for (NodeId id : ids_) {
            storage_[id] = {};
            applied_[id] = {};
            start(id, seed);
        }
    }

    const std::vector<NodeId>& ids() const { return ids_; }
    raft::RaftNode& node(NodeId id) { return *nodes_.at(id); }
    bool isUp(NodeId id) const { return nodes_.at(id) != nullptr; }

    // One unit of time: every running node ticks, then messages are
    // delivered (in random order) until the network is quiet.
    void tick() {
        for (NodeId id : ids_)
            if (isUp(id)) {
                node(id).tick();
                processReady(id);
            }
        deliverAll();
    }

    void tick(int n) {
        for (int i = 0; i < n; ++i) tick();
    }

    // Ticks until `done()` is true; returns false if it never happened.
    bool tickUntil(const std::function<bool()>& done, int maxTicks = 500) {
        for (int i = 0; i < maxTicks; ++i) {
            if (done()) return true;
            tick();
        }
        return done();
    }

    // The leader with the highest term among running nodes, if any.
    std::optional<NodeId> leader() {
        std::optional<NodeId> best;
        for (NodeId id : ids_)
            if (isUp(id) && node(id).role() == raft::Role::Leader &&
                (!best || node(id).term() > node(*best).term()))
                best = id;
        return best;
    }

    NodeId waitForLeader() {
        std::optional<NodeId> l;
        EXPECT_TRUE(tickUntil([&] { return (l = leader()).has_value(); })) << "no leader elected";
        return l.value_or(0);
    }

    // Proposes on the current leader; returns false if there is none.
    bool propose(const std::string& data) {
        auto l = leader();
        if (!l) return false;
        bool ok = node(*l).propose(data).has_value();
        processReady(*l);
        deliverAll();
        return ok;
    }

    // Commands applied by a node's state machine, in order (no-ops skipped).
    std::vector<std::string> applied(NodeId id) const {
        std::vector<std::string> out;
        for (const auto& e : applied_.at(id))
            if (e.type == raft::Entry::Type::Command) out.push_back(e.data);
        return out;
    }

    // --- Failures ---------------------------------------------------------

    // The node stops; its volatile state is gone, its disk survives.
    void crash(NodeId id) { nodes_[id].reset(); }

    // Restarts from what's on disk. The state machine is rebuilt from
    // scratch, since Raft re-delivers committed entries from index 1.
    void restart(NodeId id) {
        start(id, seed_ * 1000 + restarts_++);
        applied_[id].clear();
    }

    // Splits the network: nodes in `group` can talk only among themselves.
    void partition(const std::set<NodeId>& group) {
        cut_.clear();
        for (NodeId a : ids_)
            for (NodeId b : ids_)
                if (group.count(a) != group.count(b)) cut_.insert({a, b});
    }
    void isolate(NodeId id) { partition({id}); }
    void heal() { cut_.clear(); }

    void setDropRate(double rate) { dropRate_ = rate; }

private:
    struct Disk {
        raft::HardState hardState;
        std::vector<raft::Entry> log;  // log[i] has index i + 1
    };

    void start(NodeId id, std::uint64_t seed) {
        raft::Config cfg;
        cfg.id = id;
        cfg.peers = ids_;
        cfg.randomSeed = seed * 100 + id;
        const Disk& disk = storage_[id];
        nodes_[id] = std::make_unique<raft::RaftNode>(cfg, disk.hardState, disk.log);
    }

    // Handles a node's Ready in the required order: persist, send, apply.
    void processReady(NodeId id) {
        raft::RaftNode& n = node(id);
        if (!n.hasReady()) return;
        raft::Ready rd = n.takeReady();

        Disk& disk = storage_[id];
        if (rd.hardState) disk.hardState = *rd.hardState;
        if (!rd.entries.empty()) {
            disk.log.resize(rd.entries.front().index - 1);  // overwrite from here on
            disk.log.insert(disk.log.end(), rd.entries.begin(), rd.entries.end());
        }

        for (auto& m : rd.messages) inflight_.push_back(std::move(m));

        for (const auto& e : rd.committedEntries) {
            applied_[id].push_back(e);
            checkStateMachineSafety(id, e);
        }
        checkElectionSafety(id);
    }

    // Delivers messages in random order until none are left.
    void deliverAll() {
        int budget = 100000;  // guards against a livelock bug hanging the test
        while (!inflight_.empty() && budget-- > 0) {
            std::size_t pick = rng_() % inflight_.size();
            raft::Message m = std::move(inflight_[pick]);
            inflight_.erase(inflight_.begin() + static_cast<std::ptrdiff_t>(pick));

            if (!isUp(m.to) || cut_.count({m.from, m.to})) continue;
            if (dropRate_ > 0 && std::uniform_real_distribution<>(0, 1)(rng_) < dropRate_) continue;

            node(m.to).step(m);
            processReady(m.to);
        }
        ASSERT_GT(budget, 0) << "message storm: the network never went quiet";
    }

    void checkElectionSafety(NodeId id) {
        raft::RaftNode& n = node(id);
        if (n.role() != raft::Role::Leader) return;
        auto [it, inserted] = leaderOfTerm_.emplace(n.term(), id);
        EXPECT_EQ(it->second, id) << "two leaders in term " << n.term();
    }

    void checkStateMachineSafety(NodeId id, const raft::Entry& e) {
        auto [it, inserted] = appliedAt_.emplace(e.index, e);
        EXPECT_EQ(it->second, e) << "node " << id << " applied a different entry at index " << e.index;
    }

    std::vector<NodeId> ids_;
    std::map<NodeId, std::unique_ptr<raft::RaftNode>> nodes_;
    std::map<NodeId, Disk> storage_;
    std::map<NodeId, std::vector<raft::Entry>> applied_;
    std::deque<raft::Message> inflight_;
    std::set<std::pair<NodeId, NodeId>> cut_;
    std::mt19937_64 rng_;
    std::uint64_t seed_ = 1;
    std::uint64_t restarts_ = 1;
    double dropRate_ = 0;

    std::map<raft::Term, NodeId> leaderOfTerm_;
    std::map<raft::Index, raft::Entry> appliedAt_;
};

}  // namespace kv::test
