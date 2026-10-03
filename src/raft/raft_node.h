#pragma once

#include <cstddef>
#include <deque>
#include <map>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "raft/message.h"
#include "raft/raft_log.h"
#include "raft/types.h"

namespace kv::raft {

enum class Role { Follower, Candidate, Leader };

struct Config {
    NodeId id = 0;

    // The initial cluster: every member's id, including this node's. Only
    // used by a node that has no log and no snapshot yet (see bootstrapConfigEntry);
    // after that the configuration comes from the log.
    std::vector<NodeId> peers;
    std::map<NodeId, std::string> addresses;  // optional: "host:port" per initial member

    // Fresh cluster: record the initial configuration as log entry 1, so that
    // servers added later learn the full membership from the log. Every
    // initial node must use the same peers and addresses, which makes entry 1
    // identical everywhere. Off by default (tests that don't need membership).
    bool bootstrapConfigEntry = false;

    // A server that is about to be added to a running cluster starts with
    // joining = true. It isn't a member yet: it never starts elections and
    // doesn't vote, until the leader's log tells it that it was added.
    bool joining = false;

    // Time is measured in ticks (calls to tick()), not seconds.
    int electionTimeoutTicks = 10;  // actual timeout is random in [T, 2T)
    int heartbeatTicks = 3;         // leader sends heartbeats this often

    // One AppendEntries carries at most this many entries AND roughly this
    // many bytes of entry data (a soft limit: a single bigger entry is still
    // sent, alone). The byte limit matters: 64 entries of 1 MB each would be
    // a 64 MB message, bigger than the network layer accepts.
    std::size_t maxEntriesPerMessage = 64;
    std::size_t maxBytesPerMessage = 1024 * 1024;

    // A snapshot goes to a lagging follower in chunks of this size, and a
    // chunk that isn't acknowledged is re-sent after this many ticks.
    std::size_t snapshotChunkBytes = 256 * 1024;
    int snapshotRetryTicks = 5;

    std::uint64_t randomSeed = 0;  // 0 = use the node id
};

// A confirmed linearizable read: the application may answer the read as soon
// as it has applied every entry up to `index`.
struct ReadState {
    std::uint64_t id = 0;  // the id requestRead() returned
    Index index = 0;
};

// Everything the node wants the outside world to do, collected since the
// last takeReady(). It MUST be handled in this order:
//   1. persist `hardState`, `snapshot` and `entries`, and fsync
//   2. send `messages`
//   3. if `snapshotFromLeader`, restore the state machine from the snapshot
//   4. apply `committedEntries` to the state machine
//   5. answer reads in `readStates` (once their index has been applied)
// Persisting first matters: a message may promise something ("I voted for
// you", "I have entry 7") that must survive a crash.
struct Ready {
    std::optional<HardState> hardState;  // set only if it changed

    // Set when the log was compacted, or when a snapshot arrived from the
    // leader. Persist it, and then replace the stored log with `entries`
    // (below): everything up to snapshot->index is now covered by the snapshot.
    std::optional<Snapshot> snapshot;

    // true: this snapshot came from the leader and replaces the state
    // machine's contents. false: we made it ourselves from the state machine
    // and there is nothing to restore.
    bool snapshotFromLeader = false;

    // New or overwritten log entries. Persisting an entry at index i
    // replaces whatever was stored at i and after it. (When `snapshot` is
    // set, this is the complete log after the snapshot instead.)
    std::vector<Entry> entries;

    std::vector<Message> messages;
    std::vector<Entry> committedEntries;  // in log order
    std::vector<ReadState> readStates;

    bool empty() const {
        return !hardState && !snapshot && entries.empty() && messages.empty() && committedEntries.empty() &&
               readStates.empty();
    }
};

enum class ConfigError {
    None,
    NotLeader,  // only the leader can change the configuration
    Pending,    // the previous configuration change hasn't committed yet
    NotReady,   // this leader hasn't committed anything in its own term yet
    Invalid,    // not exactly one server added or removed
};

// The Raft consensus algorithm as a pure state machine.
//
// It never touches the network, the disk or the clock. Inputs are tick(),
// step(), propose() and friends; all output goes into a Ready. That makes it
// fully deterministic: tests can run a whole cluster in one process and
// control every message, crash and timeout.
class RaftNode {
public:
    // Starts a node from its persisted state. A brand new node passes an
    // empty HardState, an empty log and no snapshot. With a snapshot, `log`
    // holds the entries after it, and the state machine must already have
    // been restored from the snapshot.
    //
    // `appliedIndex`: a state machine that persists its own progress (the LSM
    // storage) may be AHEAD of the snapshot: it has already applied entries that
    // are still in the log. Those entries are known to be committed, and must not
    // be delivered again. Must not be beyond the end of the log.
    RaftNode(Config config, HardState hardState, std::vector<Entry> log, std::optional<Snapshot> snapshot = std::nullopt,
             Index appliedIndex = 0);

    // Advances time by one tick: may start an election or send heartbeats.
    void tick();

    // Handles a message from another node.
    void step(const Message& msg);

    // Asks the cluster to append `data` to the log. Only works on the leader:
    // returns the entry's index, or nullopt if this node isn't the leader.
    // The entry may still be lost if this leader is deposed before the entry
    // is committed; it's safe only once it shows up in committedEntries.
    std::optional<Index> propose(std::string data);

    // Changes the cluster configuration by exactly one server (added or
    // removed). Takes effect on this node as soon as the entry is appended,
    // and is committed like any entry. Returns the entry's index, or
    // nullopt with the reason in *error. One change at a time: the next one
    // is accepted only after this one has committed.
    std::optional<Index> proposeConfigChange(Members newMembers, ConfigError* error = nullptr);

    // Starts a linearizable read. Returns an id, or nullopt if this node
    // isn't the leader. Once the leader has confirmed it is still the leader
    // (a majority answered a heartbeat sent after this call), the id shows up
    // in Ready::readStates with the log index to wait for. If the leader is
    // deposed first, the id never shows up: the caller must notice that the
    // node is no longer the leader and fail the read.
    std::optional<std::uint64_t> requestRead();

    // Tells the node that the state machine has applied everything up to
    // `index` and `data` is its serialized state. The node then drops the log
    // up to `index` and reports the snapshot in the next Ready. Returns false
    // if `index` isn't something that can be compacted (not applied yet, or
    // already covered by an older snapshot).
    bool compact(Index index, std::string data);

    // Collects the pending output (see Ready).
    Ready takeReady();
    bool hasReady() const;

    NodeId id() const { return config_.id; }
    Role role() const { return role_; }
    Term term() const { return term_; }
    std::optional<NodeId> leader() const { return leader_; }
    Index commitIndex() const { return commitIndex_; }
    Index lastApplied() const { return lastApplied_; }
    const RaftLog& log() const { return log_; }

    const Members& members() const { return members_; }
    bool isMember() const { return isMemberId(config_.id); }
    Index configIndex() const { return configIndex_; }  // index of the entry that defined the current configuration
    Index snapshotIndex() const { return snapshot_ ? snapshot_->index : 0; }
    const std::optional<Snapshot>& snapshot() const { return snapshot_; }

private:
    using Body = decltype(Message::body);

    // Role transitions.
    void becomeFollower(Term term, std::optional<NodeId> leader);
    void becomeCandidate();
    void becomeLeader();

    // Message handlers. When these run, msg.term == term_.
    void handleRequestVote(const Message& msg, const RequestVote& rv);
    void handleVoteResponse(const Message& msg, const RequestVoteResponse& resp);
    void handleAppendEntries(const Message& msg, const AppendEntries& ae);
    void handleAppendResponse(const Message& msg, const AppendEntriesResponse& resp);
    void handleInstallSnapshot(const Message& msg, const InstallSnapshot& is);
    void handleSnapshotResponse(const Message& msg, const InstallSnapshotResponse& resp);

    // Leader helpers. `fromTimer` marks a heartbeat tick, as opposed to a
    // reaction to a new entry or a response.
    void sendAppend(NodeId to, bool fromTimer);
    void broadcastAppend(bool fromTimer);
    void sendSnapshotTo(NodeId to, bool fromTimer);
    void sendSnapshotChunk(NodeId to);
    void maybeAdvanceCommit();
    void processReads();
    void checkQuorum();

    // Configuration.
    void recomputeConfig();
    void applyConfig(Members members, Index index);
    void syncReplicationState();
    Members membersAt(Index index) const;
    bool isMemberId(NodeId id) const;
    Members initialMembers() const;

    void installSnapshot(Snapshot snapshot);
    bool heardFromLeaderRecently() const;

    void appendToLog(Entry entry);
    void send(NodeId to, Body body);
    void resetElectionTimer();
    std::size_t quorum() const { return members_.size() / 2 + 1; }

    Config config_;
    std::mt19937_64 rng_;

    // Persistent state (mirrored by HardState + the log + the snapshot).
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
    RaftLog log_;
    std::optional<Snapshot> snapshot_;  // the newest snapshot (made here or received)

    // The cluster configuration in effect: the latest one in the log
    // (committed or not), else the snapshot's, else the initial one.
    Members initial_;
    Members members_;
    Index configIndex_ = 0;

    // Volatile state.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    Index commitIndex_ = 0;  // highest index known to be committed
    Index lastApplied_ = 0;  // highest index handed out in committedEntries

    int electionElapsed_ = 0;  // follower: ticks since the leader was heard; leader: since the last quorum check
    int electionTimeout_ = 0;  // randomized on every reset
    int heartbeatElapsed_ = 0;

    // Candidate state.
    std::set<NodeId> votes_;

    // Leader state, per follower.
    std::map<NodeId, Index> nextIndex_;   // next entry to send
    std::map<NodeId, Index> matchIndex_;  // highest entry known to be replicated
    std::set<NodeId> activeSinceCheck_;   // followers that answered since the last quorum check
    Index termStartIndex_ = 0;            // index of this leader's first entry (its no-op)

    // Snapshot transfers in progress (leader): offset of the next chunk, and
    // ticks since the last chunk was sent.
    std::map<NodeId, std::uint64_t> snapOffset_;
    std::map<NodeId, int> snapIdle_;

    // Snapshot being received from the leader (follower).
    std::optional<Snapshot> incoming_;

    // Linearizable reads (leader).
    struct PendingRead {
        std::uint64_t id;
        std::uint64_t seq;           // heartbeat round that must be acknowledged by a majority
        std::optional<Index> index;  // commit index to wait for, once known
    };
    std::uint64_t heartbeatSeq_ = 0;
    std::map<NodeId, std::uint64_t> ackedSeq_;
    std::deque<PendingRead> reads_;
    std::uint64_t nextReadId_ = 0;
    std::vector<ReadState> readStates_;

    // Pending output for the next Ready.
    std::vector<Message> outbox_;
    Index unstableFrom_;   // log entries from here on aren't persisted yet
    HardState persisted_;  // last HardState handed out in a Ready
    bool pendingSnapshot_ = false;
    bool snapshotFromLeader_ = false;
};

}  // namespace kv::raft
