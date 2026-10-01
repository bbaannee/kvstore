#include "raft/raft_node.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace kv::raft {

RaftNode::RaftNode(Config config, HardState hardState, std::vector<Entry> log)
    : config_(std::move(config)),
      rng_(config_.randomSeed ? config_.randomSeed : config_.id),
      term_(hardState.term),
      votedFor_(hardState.votedFor),
      log_(std::move(log)),
      unstableFrom_(log_.lastIndex() + 1),  // what we were given is already on disk
      persisted_(hardState) {
    if (std::find(config_.peers.begin(), config_.peers.end(), config_.id) == config_.peers.end())
        throw std::invalid_argument("raft: peers must include the node itself");
    if (config_.heartbeatTicks >= config_.electionTimeoutTicks)
        throw std::invalid_argument("raft: heartbeats must be more frequent than election timeouts");
    resetElectionTimer();
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

void RaftNode::tick() {
    if (role_ == Role::Leader) {
        if (++heartbeatElapsed_ >= config_.heartbeatTicks) {
            heartbeatElapsed_ = 0;
            broadcastAppend();  // heartbeat (and retry for lagging followers)
        }
    } else if (++electionElapsed_ >= electionTimeout_) {
        becomeCandidate();  // haven't heard from a leader for too long
    }
}

void RaftNode::step(const Message& msg) {
    if (msg.term > term_) {
        // The sender is in a newer term, so we're out of date: whatever role
        // we had, it belongs to an old term. Only AppendEntries comes from a
        // leader, so only then do we know who leads the new term.
        bool fromLeader = std::holds_alternative<AppendEntries>(msg.body);
        becomeFollower(msg.term, fromLeader ? std::optional(msg.from) : std::nullopt);
    } else if (msg.term < term_) {
        // The sender is out of date. Answer requests with a rejection that
        // carries our term, so the sender learns it and steps down. Stale
        // responses are simply dropped.
        if (std::holds_alternative<RequestVote>(msg.body))
            send(msg.from, RequestVoteResponse{false});
        else if (std::holds_alternative<AppendEntries>(msg.body))
            send(msg.from, AppendEntriesResponse{false, 0, log_.lastIndex()});
        return;
    }

    // From here on msg.term == term_.
    if (auto* m = std::get_if<RequestVote>(&msg.body)) handleRequestVote(msg, *m);
    else if (auto* m = std::get_if<RequestVoteResponse>(&msg.body)) handleVoteResponse(msg, *m);
    else if (auto* m = std::get_if<AppendEntries>(&msg.body)) handleAppendEntries(msg, *m);
    else if (auto* m = std::get_if<AppendEntriesResponse>(&msg.body)) handleAppendResponse(msg, *m);
}

std::optional<Index> RaftNode::propose(std::string data) {
    if (role_ != Role::Leader) return std::nullopt;
    Index index = log_.lastIndex() + 1;
    appendToLog(Entry{term_, index, Entry::Type::Command, std::move(data)});
    matchIndex_[config_.id] = index;
    broadcastAppend();
    maybeAdvanceCommit();  // a single-node cluster commits immediately
    return index;
}

// ---------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------

Ready RaftNode::takeReady() {
    Ready rd;

    HardState current{term_, votedFor_};
    if (current != persisted_) {
        rd.hardState = current;
        persisted_ = current;
    }

    rd.entries = log_.slice(unstableFrom_, log_.lastIndex() + 1);
    unstableFrom_ = log_.lastIndex() + 1;

    rd.messages = std::exchange(outbox_, {});

    if (lastApplied_ < commitIndex_) {
        rd.committedEntries = log_.slice(lastApplied_ + 1, commitIndex_ + 1);
        lastApplied_ = commitIndex_;
    }
    return rd;
}

bool RaftNode::hasReady() const {
    return HardState{term_, votedFor_} != persisted_ || unstableFrom_ <= log_.lastIndex() ||
           !outbox_.empty() || lastApplied_ < commitIndex_;
}

// ---------------------------------------------------------------------------
// Role transitions
// ---------------------------------------------------------------------------

void RaftNode::becomeFollower(Term term, std::optional<NodeId> leader) {
    if (term > term_) {
        term_ = term;
        votedFor_.reset();  // a new term means a fresh vote
    }
    role_ = Role::Follower;
    leader_ = leader;
    resetElectionTimer();
}

void RaftNode::becomeCandidate() {
    ++term_;
    role_ = Role::Candidate;
    votedFor_ = config_.id;  // vote for ourselves
    leader_.reset();
    votes_ = {config_.id};
    resetElectionTimer();  // if this election splits, a new one starts later

    if (votes_.size() >= quorum()) {  // single-node cluster
        becomeLeader();
        return;
    }
    for (NodeId peer : config_.peers)
        if (peer != config_.id) send(peer, RequestVote{log_.lastIndex(), log_.lastTerm()});
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leader_ = config_.id;
    heartbeatElapsed_ = 0;
    for (NodeId peer : config_.peers) {
        nextIndex_[peer] = log_.lastIndex() + 1;  // optimistic: assume they're up to date
        matchIndex_[peer] = 0;                    // pessimistic: we know nothing yet
    }

    // A leader may only count replicas for entries from its own term (see
    // maybeAdvanceCommit). Entries left over from older terms therefore
    // can't commit until a current-term entry commits after them. The no-op
    // gives us such an entry right away, without waiting for a client.
    appendToLog(Entry{term_, log_.lastIndex() + 1, Entry::Type::NoOp, {}});
    matchIndex_[config_.id] = log_.lastIndex();

    broadcastAppend();  // announce ourselves; also replicates the no-op
    maybeAdvanceCommit();
}

// ---------------------------------------------------------------------------
// Elections
// ---------------------------------------------------------------------------

void RaftNode::handleRequestVote(const Message& msg, const RequestVote& rv) {
    // One vote per term (we may repeat a vote we already gave, in case the
    // first response was lost), and only for a candidate whose log has
    // everything ours has. That second rule guarantees that the winner
    // already holds every committed entry.
    bool canVote = !votedFor_ || *votedFor_ == msg.from;
    bool grant = canVote && log_.isUpToDate(rv.lastLogIndex, rv.lastLogTerm);
    if (grant) {
        votedFor_ = msg.from;
        resetElectionTimer();  // give the candidate time to win
    }
    send(msg.from, RequestVoteResponse{grant});
}

void RaftNode::handleVoteResponse(const Message& msg, const RequestVoteResponse& resp) {
    if (role_ != Role::Candidate || !resp.granted) return;
    votes_.insert(msg.from);
    if (votes_.size() >= quorum()) becomeLeader();
}

// ---------------------------------------------------------------------------
// Log replication
// ---------------------------------------------------------------------------

void RaftNode::handleAppendEntries(const Message& msg, const AppendEntries& ae) {
    // A valid leader exists for our term. Two leaders in one term are
    // impossible, so if we were a candidate, we lost.
    if (role_ != Role::Follower) becomeFollower(term_, msg.from);
    leader_ = msg.from;
    electionElapsed_ = 0;

    // Consistency check: we must have the entry right before the new ones.
    // If we do, by induction our logs are identical up to that point.
    if (!log_.matches(ae.prevLogIndex, ae.prevLogTerm)) {
        // Tell the leader where to look next: either our log is shorter, or
        // the entry at prevLogIndex conflicts and must go.
        Index hint = std::min(ae.prevLogIndex - 1, log_.lastIndex());
        send(msg.from, AppendEntriesResponse{false, 0, hint});
        return;
    }

    for (const Entry& e : ae.entries) {
        auto existing = log_.termAt(e.index);
        if (existing == e.term) continue;  // already have it (e.g. a duplicate message)
        if (existing) {
            // Conflict: our entry came from a leader whose term ended
            // without committing it. The current leader's log wins.
            if (e.index <= commitIndex_)
                throw std::logic_error("raft: leader tried to overwrite a committed entry");
            log_.truncateFrom(e.index);
        }
        appendToLog(e);
    }

    // We now match the leader up to the last entry it sent. Only commit
    // what we know matches; our log may still have stale entries after it.
    Index lastNew = ae.prevLogIndex + ae.entries.size();
    commitIndex_ = std::max(commitIndex_, std::min(ae.leaderCommit, lastNew));

    send(msg.from, AppendEntriesResponse{true, lastNew, 0});
}

void RaftNode::handleAppendResponse(const Message& msg, const AppendEntriesResponse& resp) {
    if (role_ != Role::Leader) return;
    NodeId peer = msg.from;

    if (resp.success) {
        // max(): responses can arrive out of order; never move backwards.
        matchIndex_[peer] = std::max(matchIndex_[peer], resp.matchIndex);
        nextIndex_[peer] = std::max(nextIndex_[peer], resp.matchIndex + 1);
        maybeAdvanceCommit();
        if (nextIndex_[peer] <= log_.lastIndex()) sendAppend(peer);  // more to send
    } else {
        // Back up and retry. The hint lets us skip straight past a short log
        // instead of stepping back one entry per round trip.
        nextIndex_[peer] = std::max<Index>(1, std::min(nextIndex_[peer] - 1, resp.rejectHint + 1));
        sendAppend(peer);
    }
}

void RaftNode::sendAppend(NodeId to) {
    Index next = nextIndex_[to];
    Index prev = next - 1;
    send(to, AppendEntries{
                 prev,
                 *log_.termAt(prev),
                 log_.slice(next, next + config_.maxEntriesPerMessage),
                 commitIndex_,
             });
}

void RaftNode::broadcastAppend() {
    for (NodeId peer : config_.peers)
        if (peer != config_.id) sendAppend(peer);
}

void RaftNode::maybeAdvanceCommit() {
    // Find the highest index stored on a majority. Only entries from our
    // own term count: an older-term entry on a majority can still be
    // overwritten by a future leader (Figure 8 in the Raft paper). Terms
    // never decrease along the log, so we stop at the first older one.
    for (Index n = log_.lastIndex(); n > commitIndex_ && log_.termAt(n) == term_; --n) {
        std::size_t replicas = 0;
        for (const auto& [peer, match] : matchIndex_)
            if (match >= n) ++replicas;
        if (replicas >= quorum()) {
            commitIndex_ = n;  // also commits everything before n
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

void RaftNode::appendToLog(Entry entry) {
    unstableFrom_ = std::min(unstableFrom_, entry.index);
    log_.append(std::move(entry));
}

void RaftNode::send(NodeId to, Body body) {
    outbox_.push_back(Message{config_.id, to, term_, std::move(body)});
}

void RaftNode::resetElectionTimer() {
    electionElapsed_ = 0;
    // Random timeouts make it likely that one node times out first and
    // wins before the others even start, instead of splitting the vote.
    auto t = static_cast<std::uint64_t>(config_.electionTimeoutTicks);
    electionTimeout_ = static_cast<int>(t + rng_() % t);
}

}  // namespace kv::raft
