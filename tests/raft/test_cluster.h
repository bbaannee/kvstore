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

#include "common/coding.h"
#include "raft/entry_codec.h"
#include "raft/raft_node.h"

namespace kv::test {

// A whole Raft cluster in one process, with a fake network and fake disks.
//
// Everything is deterministic for a given seed: message delivery order,
// message loss, and every node's election timeouts. If a test fails, the
// same seed reproduces it exactly.
//
// The "state machine" of each node is simply the list of entries it applied.
// Snapshots of it are that list, serialized.
//
// While it runs, the cluster checks Raft's safety guarantees after every
// step and fails the test the moment one is violated:
//   - Election Safety: at most one leader per term.
//   - State Machine Safety: no two nodes apply different entries at the
//     same index.
class TestCluster {
public:
    using NodeId = raft::NodeId;

    struct Options {
        std::uint64_t seed = 1;
        bool bootstrapConfig = false;  // write the initial configuration as log entry 1 (needed for membership changes)
        std::size_t snapshotEvery = 0; // snapshot once this many entries were applied since the last (0 = never)
        std::size_t snapshotChunkBytes = 256 * 1024;
    };

    explicit TestCluster(std::size_t size, std::uint64_t seed = 1) : TestCluster(size, Options{seed}) {}

    TestCluster(std::size_t size, Options options) : options_(options), rng_(options.seed) {
        for (NodeId id = 1; id <= size; ++id) initial_.push_back(id);
        ids_ = initial_;
        for (NodeId id : ids_) {
            storage_[id] = {};
            applied_[id] = {};
            start(id);
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
            if (isUp(id) && node(id).role() == raft::Role::Leader && (!best || node(id).term() > node(*best).term()))
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

    // Commands applied by a node's state machine, in order (no-ops and configuration entries skipped).
    std::vector<std::string> applied(NodeId id) const {
        std::vector<std::string> out;
        for (const auto& e : applied_.at(id))
            if (e.type == raft::Entry::Type::Command) out.push_back(e.data);
        return out;
    }

    // --- Membership ---------------------------------------------------------

    // Starts a new server that is not a member yet; it learns the cluster from the leader's log.
    void addNode(NodeId id) {
        ids_.push_back(id);
        storage_[id] = {};
        applied_[id] = {};
        joining_.insert(id);
        start(id);
    }

    // Asks the leader to change the configuration (it returns the error, if any).
    raft::ConfigError proposeConfig(raft::Members members) {
        auto l = leader();
        if (!l) return raft::ConfigError::NotLeader;
        raft::ConfigError error;
        node(*l).proposeConfigChange(std::move(members), &error);
        processReady(*l);
        deliverAll();
        return error;
    }

    // The current members as the leader sees them (including a change that hasn't committed yet).
    raft::Members leaderMembers() {
        auto l = leader();
        return l ? node(*l).members() : raft::Members{};
    }

    // --- Snapshots ----------------------------------------------------------

    // Makes a node snapshot its state machine now. Returns false if there's nothing new to cover.
    bool snapshot(NodeId id) {
        bool ok = node(id).compact(node(id).lastApplied(), serializeApplied(id));
        processReady(id);
        deliverAll();
        return ok;
    }

    // --- Failures ---------------------------------------------------------

    // The node stops; its volatile state is gone, its disk survives.
    void crash(NodeId id) { nodes_[id].reset(); }

    // Restarts from what's on disk. The state machine is rebuilt from the
    // snapshot (if any), then from the entries Raft re-delivers.
    void restart(NodeId id) {
        start(id);
        applied_[id].clear();
        if (storage_[id].snapshot) applied_[id] = deserializeApplied(storage_[id].snapshot->data);
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

    // How many snapshot chunks were sent in total (to see that InstallSnapshot really ran).
    std::size_t snapshotChunksSent() const { return chunksSent_; }
    std::size_t snapshotsInstalled() const { return installed_; }

private:
    struct Disk {
        raft::HardState hardState;
        std::optional<raft::Snapshot> snapshot;
        std::vector<raft::Entry> log;  // the entries after the snapshot
    };

    void start(NodeId id) {
        raft::Config cfg;
        cfg.id = id;
        cfg.peers = initial_;
        cfg.bootstrapConfigEntry = options_.bootstrapConfig;
        cfg.joining = joining_.count(id) > 0;
        cfg.snapshotChunkBytes = options_.snapshotChunkBytes;
        cfg.randomSeed = options_.seed * 100 + id + restarts_[id]++ * 100000;
        const Disk& disk = storage_[id];
        nodes_[id] = std::make_unique<raft::RaftNode>(cfg, disk.hardState, disk.log, disk.snapshot);
    }

    std::string serializeApplied(NodeId id) const {
        std::string out;
        putFixed64(out, applied_.at(id).size());
        for (const auto& e : applied_.at(id)) raft::putEntry(out, e);
        return out;
    }

    std::vector<raft::Entry> deserializeApplied(const std::string& bytes) const {
        ByteReader in(bytes);
        std::uint64_t count = 0;
        std::vector<raft::Entry> out;
        EXPECT_TRUE(in.getFixed64(count));
        for (std::uint64_t i = 0; i < count; ++i) {
            raft::Entry e;
            EXPECT_TRUE(raft::getEntry(in, e));
            out.push_back(std::move(e));
        }
        return out;
    }

    // Handles a node's Ready in the required order: persist, send, apply.
    void processReady(NodeId id) {
        while (isUp(id) && node(id).hasReady()) {
            raft::RaftNode& n = node(id);
            raft::Ready rd = n.takeReady();

            Disk& disk = storage_[id];
            if (rd.hardState) disk.hardState = *rd.hardState;
            if (rd.snapshot) {
                disk.snapshot = rd.snapshot;
                disk.log = rd.entries;  // the whole log after the snapshot
            } else if (!rd.entries.empty()) {
                const raft::Index base = disk.snapshot ? disk.snapshot->index : 0;
                disk.log.resize(rd.entries.front().index - 1 - base);  // overwrite from here on
                disk.log.insert(disk.log.end(), rd.entries.begin(), rd.entries.end());
            }

            for (auto& m : rd.messages) {
                if (const auto* chunk = std::get_if<raft::InstallSnapshot>(&m.body)) chunksSent_ += chunk->done ? 1 : 0;
                inflight_.push_back(std::move(m));
            }

            if (rd.snapshot && rd.snapshotFromLeader) {
                applied_[id] = deserializeApplied(rd.snapshot->data);
                ++installed_;
            }
            for (const auto& e : rd.committedEntries) {
                applied_[id].push_back(e);
                checkStateMachineSafety(id, e);
            }
            checkElectionSafety(id);

            const raft::Index sinceSnapshot = n.lastApplied() - n.snapshotIndex();
            if (options_.snapshotEvery && sinceSnapshot >= options_.snapshotEvery)
                n.compact(n.lastApplied(), serializeApplied(id));
        }
    }

    // Delivers messages in random order until none are left.
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

    Options options_;
    std::vector<NodeId> initial_;
    std::vector<NodeId> ids_;
    std::set<NodeId> joining_;
    std::map<NodeId, std::unique_ptr<raft::RaftNode>> nodes_;
    std::map<NodeId, Disk> storage_;
    std::map<NodeId, std::vector<raft::Entry>> applied_;
    std::map<NodeId, int> restarts_;
    std::deque<raft::Message> inflight_;
    std::set<std::pair<NodeId, NodeId>> cut_;
    std::mt19937_64 rng_;
    double dropRate_ = 0;
    std::size_t chunksSent_ = 0;
    std::size_t installed_ = 0;

    std::map<raft::Term, NodeId> leaderOfTerm_;
    std::map<raft::Index, raft::Entry> appliedAt_;
};

}  // namespace kv::test
