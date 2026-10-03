#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <vector>

#include "raft/raft_node.h"
#include "raft/raft_storage.h"
#include "server/command.h"
#include "server/kv_state_machine.h"
#include "storage/log_record.h"

namespace kv::server {

enum class RequestStatus {
    Committed,  // the command was committed and applied; the result is valid
    NotLeader,  // this node can't take the request; try the node in leader()
    Aborted,    // we were deposed and our entry was overwritten; it did NOT happen
    Rejected,   // the request is invalid right now (e.g. a membership change while another is pending); it did NOT happen
};

using Callback = std::function<void(RequestStatus, const ApplyResult&)>;

// One server of the replicated store: a RaftNode plus the real-world pieces
// it deliberately knows nothing about, a disk and a state machine.
//
//   tick() / step() / submit() ──▶ RaftNode ──▶ Ready
//                                                 │ 1. persist hard state, snapshot, entries (fsync)
//                                                 │ 2. queue messages for the network
//                                                 │ 3. restore the state machine from a snapshot, if one arrived
//                                                 └ 4. apply committed entries, answer clients
//
// It still doesn't touch the network or a clock: the caller feeds it ticks
// and incoming messages, and collects outgoing ones with takeOutgoing().
class ReplicatedNode {
public:
    // Where the key-value data lives.
    enum class Engine {
        Memory,  // a hash map: rebuilt from the Raft snapshot and log after a restart
        Lsm,     // an LSM tree in <dir>/state: survives a restart by itself
    };

    struct Options {
        bool syncOnAppend = true;  // false is for tests only

        // Take a snapshot (and drop the log behind it) once this many entries
        // have been applied since the last one. 0 disables snapshots.
        std::size_t snapshotThreshold = 0;

        std::size_t maxSessions = 10'000;  // see KvStateMachine

        Engine engine = Engine::Memory;
        lsm::Options lsm;  // tuning for Engine::Lsm (its directory is chosen by the node)
    };

    // Opens (or creates) the node's data in `dir` and recovers from it. The
    // state machine starts from its newest snapshot (memory engine), or from
    // wherever the LSM tree says it got to; the log entries after that are
    // applied as the cluster re-commits them.
    ReplicatedNode(const std::string& dir, raft::Config config, Options options);
    ReplicatedNode(const std::string& dir, raft::Config config, bool syncOnAppend = true)
        : ReplicatedNode(dir, std::move(config), withSync(syncOnAppend)) {}

    void tick();
    void step(const raft::Message& msg);

    // Submits a command (Put, Del or Get) through the Raft log. The callback is
    // called exactly once, but possibly never if this node is cut off and the
    // entry is never decided, so callers need a timeout.
    // Not the leader: the callback runs immediately with NotLeader.
    void submit(const Command& command, Callback callback);

    // Anonymous command: no client identity, so no deduplication.
    void submit(const storage::LogRecord& op, Callback callback) { submit(Command{0, 0, op}, std::move(callback)); }

    // A linearizable read that doesn't go through the log (ReadIndex): the
    // leader confirms it is still the leader with one round of heartbeats,
    // then answers from the state machine once it has applied everything that
    // was committed when the read arrived. Same callback contract as submit();
    // NotLeader also covers "was the leader, but lost it before confirming".
    void read(const std::string& key, Callback callback);

    // Adds or removes one server (see RaftNode::proposeConfigChange). The
    // callback runs when the change has committed. Rejected: another change
    // is still pending, or the change is invalid (not exactly one server).
    void changeMembership(raft::Members newMembers, Callback callback);

    // Takes a snapshot of the current state right now (what the threshold
    // does automatically). Returns false if there is nothing new to cover.
    bool snapshotNow();

    // Group commit. Normally every tick(), step(), submit()... persists its own
    // output (one fsync each) before returning. With batching on, they only
    // record what Raft wants to do, and nothing is persisted, sent or answered
    // until the owner calls flush(). A server handling many requests per event
    // loop iteration then pays ONE fsync for all of them. Persist-before-send
    // still holds: flush() does the persisting before anything leaves the node.
    void setBatching(bool on) { batching_ = on; }
    void flush() { processReady(); }

    // Messages to deliver to other nodes.
    std::vector<raft::Message> takeOutgoing() { return std::exchange(outgoing_, {}); }

    const raft::RaftNode& raft() const { return raft_; }
    const KvStateMachine& state() const { return state_; }
    raft::Index appliedIndex() const { return appliedIndex_; }
    std::size_t pendingRequests() const { return pending_.size(); }

private:
    static Options withSync(bool syncOnAppend) {
        Options o;
        o.syncOnAppend = syncOnAppend;
        return o;
    }

    // Handles everything the RaftNode has queued, in the required order,
    // unless batching is on (then flush() does it).
    void maybeProcessReady() {
        if (!batching_) processReady();
    }
    void processReady();
    void maybeSnapshot();
    void serveReadyReads();
    void failUnconfirmedReads();

    Options options_;
    // The order matters: each member is initialized from the ones above it.
    raft::RaftStorage storage_;  // recovers the term, vote, log and snapshot
    KvStateMachine state_;
    raft::Index appliedIndex_ = 0;  // the last log entry the state machine reflects; where Raft resumes
    raft::RaftNode raft_;
    bool batching_ = false;

    // A request waiting for its entry to be applied. We remember the term
    // we proposed in: if the entry that ends up at `index` has a different
    // term, it's not ours; a new leader overwrote it.
    struct Pending {
        raft::Term term;
        Callback callback;
    };
    std::map<raft::Index, Pending> pending_;

    // A linearizable read: first waiting for Raft to confirm leadership, then
    // for the state machine to reach the confirmed index.
    struct PendingRead {
        std::string key;
        raft::Term term;
        Callback callback;
    };
    std::map<std::uint64_t, PendingRead> unconfirmedReads_;      // by Raft's read id
    std::multimap<raft::Index, PendingRead> confirmedReads_;     // by the index to wait for

    std::vector<raft::Message> outgoing_;
};

}  // namespace kv::server
