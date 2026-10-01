#pragma once

#include <cstddef>
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
    std::vector<NodeId> peers;  // every node in the cluster, including this one

    // Time is measured in ticks (calls to tick()), not seconds.
    int electionTimeoutTicks = 10;  // actual timeout is random in [T, 2T)
    int heartbeatTicks = 3;         // leader sends heartbeats this often

    std::size_t maxEntriesPerMessage = 64;
    std::uint64_t randomSeed = 0;  // 0 = use the node id
};

// Everything the node wants the outside world to do, collected since the
// last takeReady(). It MUST be handled in this order:
//   1. persist `hardState` (if set) and `entries`, and fsync
//   2. send `messages`
//   3. apply `committedEntries` to the state machine
// Persisting first matters: a message may promise something ("I voted for
// you", "I have entry 7") that must survive a crash.
struct Ready {
    std::optional<HardState> hardState;  // set only if it changed

    // New or overwritten log entries. Persisting an entry at index i
    // replaces whatever was stored at i and after it.
    std::vector<Entry> entries;

    std::vector<Message> messages;
    std::vector<Entry> committedEntries;  // in log order

    bool empty() const {
        return !hardState && entries.empty() && messages.empty() && committedEntries.empty();
    }
};

// The Raft consensus algorithm as a pure state machine.
//
// It never touches the network, the disk or the clock. Inputs are tick(),
// step() and propose(); all output goes into a Ready. That makes it fully
// deterministic: tests can run a whole cluster in one process and control
// every message, crash and timeout.
class RaftNode {
public:
    // Starts a node from its persisted state. A brand new node passes an
    // empty HardState and an empty log.
    RaftNode(Config config, HardState hardState, std::vector<Entry> log);

    // Advances time by one tick: may start an election or send heartbeats.
    void tick();

    // Handles a message from another node.
    void step(const Message& msg);

    // Asks the cluster to append `data` to the log. Only works on the leader:
    // returns the entry's index, or nullopt if this node isn't the leader.
    // The entry may still be lost if this leader is deposed before the entry
    // is committed; it's safe only once it shows up in committedEntries.
    std::optional<Index> propose(std::string data);

    // Collects the pending output (see Ready).
    Ready takeReady();
    bool hasReady() const;

    NodeId id() const { return config_.id; }
    Role role() const { return role_; }
    Term term() const { return term_; }
    std::optional<NodeId> leader() const { return leader_; }
    Index commitIndex() const { return commitIndex_; }
    const RaftLog& log() const { return log_; }

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

    // Leader helpers.
    void sendAppend(NodeId to);
    void broadcastAppend();
    void maybeAdvanceCommit();

    void appendToLog(Entry entry);
    void send(NodeId to, Body body);
    void resetElectionTimer();
    std::size_t quorum() const { return config_.peers.size() / 2 + 1; }

    Config config_;
    std::mt19937_64 rng_;

    // Persistent state (mirrored by HardState + the log).
    Term term_ = 0;
    std::optional<NodeId> votedFor_;
    RaftLog log_;

    // Volatile state.
    Role role_ = Role::Follower;
    std::optional<NodeId> leader_;
    Index commitIndex_ = 0;  // highest index known to be committed
    Index lastApplied_ = 0;  // highest index handed out in committedEntries

    int electionElapsed_ = 0;
    int electionTimeout_ = 0;  // randomized on every reset
    int heartbeatElapsed_ = 0;

    // Candidate state.
    std::set<NodeId> votes_;

    // Leader state, per follower.
    std::map<NodeId, Index> nextIndex_;   // next entry to send
    std::map<NodeId, Index> matchIndex_;  // highest entry known to be replicated

    // Pending output for the next Ready.
    std::vector<Message> outbox_;
    Index unstableFrom_;   // log entries from here on aren't persisted yet
    HardState persisted_;  // last HardState handed out in a Ready
};

}  // namespace kv::raft
