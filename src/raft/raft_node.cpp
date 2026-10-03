#include "raft/raft_node.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

#include "raft/members_codec.h"

namespace kv::raft {

RaftNode::RaftNode(Config config, HardState hardState, std::vector<Entry> log, std::optional<Snapshot> snapshot,
                   Index appliedIndex)
    : config_(std::move(config)),
      rng_(config_.randomSeed ? config_.randomSeed : config_.id),
      term_(hardState.term),
      votedFor_(hardState.votedFor),
      log_(snapshot ? RaftLog(snapshot->index, snapshot->term, std::move(log)) : RaftLog(std::move(log))),
      snapshot_(std::move(snapshot)),
      unstableFrom_(log_.lastIndex() + 1),  // what we were given is already on disk
      persisted_(hardState) {
    const bool listed =
        std::find(config_.peers.begin(), config_.peers.end(), config_.id) != config_.peers.end();
    if (!config_.joining && !listed) throw std::invalid_argument("raft: peers must include the node itself");
    if (config_.heartbeatTicks >= config_.electionTimeoutTicks)
        throw std::invalid_argument("raft: heartbeats must be more frequent than election timeouts");

    initial_ = config_.joining ? Members{} : initialMembers();

    // Everything up to the snapshot is applied by definition (the state
    // machine was restored from it before we were created).
    if (snapshot_) commitIndex_ = lastApplied_ = snapshot_->index;

    // A state machine that kept its own progress may have applied more. Whatever it has
    // applied was committed, so it counts as committed here too.
    if (appliedIndex > lastApplied_) {
        if (appliedIndex > log_.lastIndex())
            throw std::invalid_argument("raft: the state machine is ahead of the log it was given");
        commitIndex_ = lastApplied_ = appliedIndex;
    }

    // A brand new cluster: the first log entry records who the members are,
    // so servers added later learn it by replicating the log.
    if (config_.bootstrapConfigEntry && !config_.joining && log_.lastIndex() == 0 && !snapshot_)
        appendToLog(Entry{0, 1, Entry::Type::Config, encodeMembers(initial_)});

    recomputeConfig();
    resetElectionTimer();
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

void RaftNode::tick() {
    if (role_ == Role::Leader) {
        if (++heartbeatElapsed_ >= config_.heartbeatTicks) {
            heartbeatElapsed_ = 0;
            broadcastAppend(/*fromTimer=*/true);  // heartbeat (and retry for lagging followers)
        }
        if (++electionElapsed_ >= config_.electionTimeoutTicks) {
            electionElapsed_ = 0;
            checkQuorum();
        }
        return;
    }

    // Time passes for everyone, members or not: how long ago we heard a leader
    // decides whether we still honour it (see heardFromLeaderRecently). Stopping
    // the clock for a non-member would make it ignore vote requests forever.
    if (electionElapsed_ < electionTimeout_) ++electionElapsed_;

    // But a server outside the configuration never starts an election itself.
    if (isMember() && electionElapsed_ >= electionTimeout_) becomeCandidate();  // no leader for too long
}

void RaftNode::step(const Message& msg) {
    // Leader stickiness: while we have a leader we're happy with, ignore
    // requests to start a new term. Otherwise one disruptive server (say, a
    // removed one that never hears heartbeats and keeps timing out) could
    // depose a healthy leader just by announcing a higher term.
    if (std::holds_alternative<RequestVote>(msg.body) && msg.term > term_ && heardFromLeaderRecently()) return;

    if (msg.term > term_) {
        // The sender is in a newer term, so we're out of date: whatever role
        // we had, it belongs to an old term. Only AppendEntries and
        // InstallSnapshot come from a leader, so only then do we know who
        // leads the new term.
        const bool fromLeader =
            std::holds_alternative<AppendEntries>(msg.body) || std::holds_alternative<InstallSnapshot>(msg.body);
        becomeFollower(msg.term, fromLeader ? std::optional(msg.from) : std::nullopt);
    } else if (msg.term < term_) {
        // The sender is out of date. Answer requests with a rejection that
        // carries our term, so the sender learns it and steps down. Stale
        // responses are simply dropped.
        if (std::holds_alternative<RequestVote>(msg.body)) {
            send(msg.from, RequestVoteResponse{false});
        } else if (const auto* ae = std::get_if<AppendEntries>(&msg.body)) {
            send(msg.from, AppendEntriesResponse{false, 0, log_.lastIndex(), ae->seq});
        } else if (const auto* is = std::get_if<InstallSnapshot>(&msg.body)) {
            send(msg.from, InstallSnapshotResponse{is->lastIndex, 0, false});
        }
        return;
    }

    // From here on msg.term == term_.
    if (auto* m = std::get_if<RequestVote>(&msg.body)) handleRequestVote(msg, *m);
    else if (auto* m = std::get_if<RequestVoteResponse>(&msg.body)) handleVoteResponse(msg, *m);
    else if (auto* m = std::get_if<AppendEntries>(&msg.body)) handleAppendEntries(msg, *m);
    else if (auto* m = std::get_if<AppendEntriesResponse>(&msg.body)) handleAppendResponse(msg, *m);
    else if (auto* m = std::get_if<InstallSnapshot>(&msg.body)) handleInstallSnapshot(msg, *m);
    else if (auto* m = std::get_if<InstallSnapshotResponse>(&msg.body)) handleSnapshotResponse(msg, *m);
}

std::optional<Index> RaftNode::propose(std::string data) {
    // A leader that has been removed from the configuration is only waiting
    // for that change to commit; it takes no new work.
    if (role_ != Role::Leader || !isMember()) return std::nullopt;
    Index index = log_.lastIndex() + 1;
    appendToLog(Entry{term_, index, Entry::Type::Command, std::move(data)});
    broadcastAppend(false);
    maybeAdvanceCommit();  // a single-node cluster commits immediately
    return index;
}

std::optional<Index> RaftNode::proposeConfigChange(Members newMembers, ConfigError* error) {
    auto fail = [&](ConfigError e) -> std::optional<Index> {
        if (error) *error = e;
        return std::nullopt;
    };
    if (error) *error = ConfigError::None;

    if (role_ != Role::Leader) return fail(ConfigError::NotLeader);
    // Two changes in flight at once could let two disjoint majorities exist;
    // changing one server at a time, each fully committed, rules that out.
    if (configIndex_ > commitIndex_) return fail(ConfigError::Pending);
    // A new leader doesn't know the real commit index until something from its
    // own term commits. Until then it can't tell whether the old change is done.
    if (commitIndex_ < termStartIndex_) return fail(ConfigError::NotReady);

    newMembers = sortedMembers(std::move(newMembers));
    if (newMembers.empty()) return fail(ConfigError::Invalid);
    for (std::size_t i = 1; i < newMembers.size(); ++i)
        if (newMembers[i - 1].id == newMembers[i].id) return fail(ConfigError::Invalid);

    // Exactly one server must differ (an address change counts as a removal plus an addition).
    std::size_t differences = 0;
    for (const Member& m : newMembers)
        if (std::find(members_.begin(), members_.end(), m) == members_.end()) ++differences;
    for (const Member& m : members_)
        if (std::find(newMembers.begin(), newMembers.end(), m) == newMembers.end()) ++differences;
    if (differences != 1) return fail(ConfigError::Invalid);

    Index index = log_.lastIndex() + 1;
    appendToLog(Entry{term_, index, Entry::Type::Config, encodeMembers(newMembers)});
    applyConfig(std::move(newMembers), index);  // takes effect now, before it commits
    broadcastAppend(false);
    maybeAdvanceCommit();
    return index;
}

std::optional<std::uint64_t> RaftNode::requestRead() {
    if (role_ != Role::Leader || !isMember()) return std::nullopt;

    // A read is safe once a majority has acknowledged a heartbeat round that
    // STARTED after the read arrived: that proves no newer leader existed
    // when the read arrived (a newer leader needs a majority's votes, and
    // those voters would have rejected our heartbeat).
    const std::uint64_t id = ++nextReadId_;
    reads_.push_back(PendingRead{id, ++heartbeatSeq_, std::nullopt});
    broadcastAppend(false);
    processReads();
    return id;
}

bool RaftNode::compact(Index index, std::string data) {
    if (index > lastApplied_ || index <= log_.baseIndex()) return false;
    auto term = log_.termAt(index);
    if (!term) return false;

    Snapshot snap{index, *term, membersAt(index), std::move(data)};
    log_.compactTo(index, *term);
    snapshot_ = std::move(snap);
    pendingSnapshot_ = true;
    snapshotFromLeader_ = false;

    // A transfer of an older snapshot is moot: restart it from the new one.
    snapOffset_.clear();
    snapIdle_.clear();
    return true;
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

    if (pendingSnapshot_) {
        // The stored log must be replaced as a whole: report everything we
        // still hold after the snapshot, persisted before or not.
        rd.snapshot = snapshot_;
        rd.snapshotFromLeader = snapshotFromLeader_;
        rd.entries = log_.slice(log_.firstIndex(), log_.lastIndex() + 1);
        pendingSnapshot_ = false;
        snapshotFromLeader_ = false;
    } else {
        rd.entries = log_.slice(unstableFrom_, log_.lastIndex() + 1);
    }
    unstableFrom_ = log_.lastIndex() + 1;

    rd.messages = std::exchange(outbox_, {});
    rd.readStates = std::exchange(readStates_, {});

    if (lastApplied_ < commitIndex_) {
        rd.committedEntries = log_.slice(lastApplied_ + 1, commitIndex_ + 1);
        lastApplied_ = commitIndex_;
    }
    return rd;
}

bool RaftNode::hasReady() const {
    return HardState{term_, votedFor_} != persisted_ || pendingSnapshot_ || unstableFrom_ <= log_.lastIndex() ||
           !outbox_.empty() || !readStates_.empty() || lastApplied_ < commitIndex_;
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
    reads_.clear();  // reads that weren't confirmed yet will never be
    snapOffset_.clear();
    snapIdle_.clear();
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
    for (const Member& m : members_)
        if (m.id != config_.id) send(m.id, RequestVote{log_.lastIndex(), log_.lastTerm()});
}

void RaftNode::becomeLeader() {
    role_ = Role::Leader;
    leader_ = config_.id;
    heartbeatElapsed_ = 0;
    electionElapsed_ = 0;
    activeSinceCheck_.clear();
    ackedSeq_.clear();
    snapOffset_.clear();
    snapIdle_.clear();
    reads_.clear();

    nextIndex_.clear();
    matchIndex_.clear();
    syncReplicationState();  // optimistic: assume followers are up to date; we know nothing about their match yet

    // A leader may only count replicas for entries from its own term (see
    // maybeAdvanceCommit). Entries left over from older terms therefore
    // can't commit until a current-term entry commits after them. The no-op
    // gives us such an entry right away, without waiting for a client.
    termStartIndex_ = log_.lastIndex() + 1;
    appendToLog(Entry{term_, termStartIndex_, Entry::Type::NoOp, {}});

    broadcastAppend(false);  // announce ourselves; also replicates the no-op
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
    //
    // We deliberately do NOT look at the configuration here. Safety needs only
    // those two rules. And refusing would be a liveness bug: a server that was
    // just added but has not yet learned that (it has no log) counts towards the
    // majority of the new configuration, so its vote can be the one that lets a
    // leader be elected, and that leader is the only one who can tell it. Who
    // counts is decided by the candidate, which only tallies votes from members
    // of its own configuration (handleVoteResponse).
    const bool canVote = !votedFor_ || *votedFor_ == msg.from;
    const bool grant = canVote && log_.isUpToDate(rv.lastLogIndex, rv.lastLogTerm);
    if (grant) {
        votedFor_ = msg.from;
        resetElectionTimer();  // give the candidate time to win
    }
    send(msg.from, RequestVoteResponse{grant});
}

void RaftNode::handleVoteResponse(const Message& msg, const RequestVoteResponse& resp) {
    if (role_ != Role::Candidate || !resp.granted || !isMemberId(msg.from)) return;
    votes_.insert(msg.from);
    if (votes_.size() >= quorum()) becomeLeader();
}

// True if we have a leader we have no reason to doubt. Used to turn down
// requests to start a new term.
bool RaftNode::heardFromLeaderRecently() const {
    // A leader that can't reach a majority steps down by itself (checkQuorum),
    // so a sitting leader can be trusted to be current.
    if (role_ == Role::Leader) return true;
    // Followers: a heartbeat within the last half election timeout. Real
    // elections happen after a full timeout of silence, so they aren't blocked.
    return leader_.has_value() && electionElapsed_ < config_.electionTimeoutTicks / 2;
}

// A leader that hasn't heard from a majority for a whole election timeout
// may be cut off from the cluster, with a new leader already elected on the
// other side. It steps down instead of carrying on believing it leads.
void RaftNode::checkQuorum() {
    std::size_t active = isMember() ? 1 : 0;
    for (NodeId peer : activeSinceCheck_)
        if (isMemberId(peer)) ++active;
    activeSinceCheck_.clear();
    if (active < quorum()) becomeFollower(term_, std::nullopt);
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

    Index prev = ae.prevLogIndex;
    Term prevTerm = ae.prevLogTerm;
    const std::vector<Entry>* entries = &ae.entries;
    std::vector<Entry> trimmed;

    // Part of the message may be about entries our snapshot already covers.
    // Those are committed, so they are the same on every server: skip them
    // and continue from the base.
    if (prev < log_.baseIndex()) {
        const Index covered = log_.baseIndex() - prev;
        if (covered >= ae.entries.size()) {
            send(msg.from, AppendEntriesResponse{true, log_.baseIndex(), 0, ae.seq});
            return;
        }
        trimmed.assign(ae.entries.begin() + static_cast<std::ptrdiff_t>(covered), ae.entries.end());
        entries = &trimmed;
        prev = log_.baseIndex();
        prevTerm = log_.baseTerm();
    }

    // Consistency check: we must have the entry right before the new ones.
    // If we do, by induction our logs are identical up to that point.
    if (!log_.matches(prev, prevTerm)) {
        // Tell the leader where to look next: either our log is shorter, or
        // the entry at prevLogIndex conflicts and must go.
        const Index hint = std::min(prev - 1, log_.lastIndex());
        send(msg.from, AppendEntriesResponse{false, 0, hint, ae.seq});
        return;
    }

    bool truncated = false, sawConfig = false;
    for (const Entry& e : *entries) {
        auto existing = log_.termAt(e.index);
        if (existing == e.term) continue;  // already have it (e.g. a duplicate message)
        if (existing) {
            // Conflict: our entry came from a leader whose term ended
            // without committing it. The current leader's log wins.
            if (e.index <= commitIndex_) throw std::logic_error("raft: leader tried to overwrite a committed entry");
            log_.truncateFrom(e.index);
            truncated = true;
        }
        sawConfig |= e.type == Entry::Type::Config;
        appendToLog(e);
    }
    // The configuration is whatever the latest Config entry in the log says,
    // committed or not. Overwriting entries can undo a configuration change.
    if (truncated || sawConfig) recomputeConfig();

    // We now match the leader up to the last entry it sent. Only commit
    // what we know matches; our log may still have stale entries after it.
    const Index lastNew = prev + entries->size();
    commitIndex_ = std::max(commitIndex_, std::min(ae.leaderCommit, lastNew));

    send(msg.from, AppendEntriesResponse{true, lastNew, 0, ae.seq});
}

void RaftNode::handleAppendResponse(const Message& msg, const AppendEntriesResponse& resp) {
    if (role_ != Role::Leader) return;
    const NodeId peer = msg.from;
    if (!isMemberId(peer)) return;

    // Any answer in our term shows the follower accepts us as its leader.
    activeSinceCheck_.insert(peer);
    ackedSeq_[peer] = std::max(ackedSeq_[peer], resp.seq);

    if (resp.success) {
        // max(): responses can arrive out of order; never move backwards.
        matchIndex_[peer] = std::max(matchIndex_[peer], resp.matchIndex);
        nextIndex_[peer] = std::max(nextIndex_[peer], resp.matchIndex + 1);
        maybeAdvanceCommit();
        if (nextIndex_[peer] <= log_.lastIndex()) sendAppend(peer, false);  // more to send
    } else {
        // Back up and retry. The hint lets us skip straight past a short log
        // instead of stepping back one entry per round trip.
        nextIndex_[peer] = std::max<Index>(1, std::min(nextIndex_[peer] - 1, resp.rejectHint + 1));
        sendAppend(peer, false);
    }
    processReads();
}

void RaftNode::sendAppend(NodeId to, bool fromTimer) {
    Index next = nextIndex_[to];

    // What the follower needs next was compacted away: only a snapshot helps.
    if (next <= log_.baseIndex()) {
        sendSnapshotTo(to, fromTimer);
        return;
    }
    // Entries are enough again: any snapshot transfer to this follower is over.
    snapOffset_.erase(to);
    snapIdle_.erase(to);

    Index prev = next - 1;

    std::vector<Entry> entries;
    std::size_t bytes = 0;
    for (Index i = next; i <= log_.lastIndex() && entries.size() < config_.maxEntriesPerMessage; ++i) {
        const Entry& e = log_.at(i);
        if (!entries.empty() && bytes + e.data.size() > config_.maxBytesPerMessage) break;
        bytes += e.data.size();
        entries.push_back(e);
    }

    // Pipelining: assume the entries arrive, and move on. Without this,
    // every heartbeat would re-send everything the follower hasn't
    // acknowledged yet, so a big entry that takes a while to arrive gets
    // sent again and again, burying the follower in duplicates.
    //
    // If the assumption is wrong (the message was lost, or the follower's log
    // differs), the next AppendEntries has a prevLogIndex the follower
    // doesn't match; it rejects it with a hint, and handleAppendResponse
    // moves nextIndex back. The regular heartbeat guarantees that next
    // message even when there are no new entries.
    nextIndex_[to] = next + entries.size();

    send(to, AppendEntries{prev, *log_.termAt(prev), std::move(entries), commitIndex_, heartbeatSeq_});
}

void RaftNode::broadcastAppend(bool fromTimer) {
    for (const Member& m : members_)
        if (m.id != config_.id) sendAppend(m.id, fromTimer);
}

void RaftNode::sendSnapshotTo(NodeId to, bool fromTimer) {
    auto transfer = snapIdle_.find(to);
    if (transfer == snapIdle_.end()) {
        snapOffset_[to] = 0;  // no transfer yet: start one
        snapIdle_[to] = 0;
    } else if (fromTimer) {
        // A transfer is under way; its acknowledgements drive it. Only if
        // they stop coming (a lost chunk or ack) do we re-send, after a pause.
        if (++transfer->second < config_.snapshotRetryTicks) return;
        transfer->second = 0;
    } else {
        return;
    }
    sendSnapshotChunk(to);
}

void RaftNode::sendSnapshotChunk(NodeId to) {
    const Snapshot& s = *snapshot_;
    const std::uint64_t offset = std::min<std::uint64_t>(snapOffset_[to], s.data.size());
    const std::size_t length = std::min<std::size_t>(config_.snapshotChunkBytes, s.data.size() - offset);

    InstallSnapshot chunk;
    chunk.lastIndex = s.index;
    chunk.lastTerm = s.term;
    chunk.members = s.members;
    chunk.offset = offset;
    chunk.data = s.data.substr(offset, length);
    chunk.done = offset + length >= s.data.size();
    snapIdle_[to] = 0;
    send(to, std::move(chunk));
}

void RaftNode::handleSnapshotResponse(const Message& msg, const InstallSnapshotResponse& resp) {
    if (role_ != Role::Leader || !snapshot_ || resp.lastIndex != snapshot_->index) return;  // stale
    const NodeId peer = msg.from;
    if (!isMemberId(peer)) return;
    activeSinceCheck_.insert(peer);

    if (resp.done) {
        // The follower has everything up to the snapshot: back to normal replication.
        matchIndex_[peer] = std::max(matchIndex_[peer], snapshot_->index);
        nextIndex_[peer] = std::max(nextIndex_[peer], snapshot_->index + 1);
        snapOffset_.erase(peer);
        snapIdle_.erase(peer);
        maybeAdvanceCommit();
        sendAppend(peer, false);
    } else if (snapIdle_.count(peer)) {
        snapOffset_[peer] = resp.nextOffset;  // continue where the follower says
        sendSnapshotChunk(peer);
    }
}

void RaftNode::handleInstallSnapshot(const Message& msg, const InstallSnapshot& is) {
    if (role_ != Role::Follower) becomeFollower(term_, msg.from);
    leader_ = msg.from;
    electionElapsed_ = 0;

    // We already have everything this snapshot covers (a delayed duplicate,
    // or we caught up by other means): tell the leader we're done.
    if (is.lastIndex <= commitIndex_) {
        send(msg.from, InstallSnapshotResponse{is.lastIndex, 0, true});
        return;
    }

    const bool sameTransfer = incoming_ && incoming_->index == is.lastIndex && incoming_->term == is.lastTerm;
    if (is.offset == 0) {
        incoming_ = Snapshot{is.lastIndex, is.lastTerm, is.members, {}};  // (re)start the transfer
    } else if (!sameTransfer) {
        send(msg.from, InstallSnapshotResponse{is.lastIndex, 0, false});  // we missed the start: ask for it
        return;
    }

    if (is.offset > incoming_->data.size()) {  // a chunk went missing: ask for the one we need
        send(msg.from, InstallSnapshotResponse{is.lastIndex, incoming_->data.size(), false});
        return;
    }
    if (is.offset == incoming_->data.size()) incoming_->data += is.data;  // (an older offset is a duplicate: ignore it)

    if (is.done && incoming_->data.size() == is.offset + is.data.size()) {
        installSnapshot(std::move(*incoming_));
        incoming_.reset();
        send(msg.from, InstallSnapshotResponse{is.lastIndex, 0, true});
    } else {
        send(msg.from, InstallSnapshotResponse{is.lastIndex, incoming_->data.size(), false});
    }
}

void RaftNode::installSnapshot(Snapshot snapshot) {
    // If our log agrees with the snapshot at its last index, what follows is
    // still valid and we keep it. Otherwise the log conflicts: discard it all.
    if (log_.matches(snapshot.index, snapshot.term)) log_.compactTo(snapshot.index, snapshot.term);
    else log_.resetTo(snapshot.index, snapshot.term);

    commitIndex_ = snapshot.index;  // (the caller checked it is beyond our old commit index)
    lastApplied_ = snapshot.index;  // the state machine will be restored to exactly this point
    snapshot_ = std::move(snapshot);
    pendingSnapshot_ = true;
    snapshotFromLeader_ = true;
    recomputeConfig();
}

void RaftNode::maybeAdvanceCommit() {
    // Find the highest index stored on a majority. Only entries from our
    // own term count: an older-term entry on a majority can still be
    // overwritten by a future leader (Figure 8 in the Raft paper). Terms
    // never decrease along the log, so we stop at the first older one.
    for (Index n = log_.lastIndex(); n > commitIndex_ && log_.termAt(n) == term_; --n) {
        std::size_t replicas = isMember() ? 1 : 0;  // our own log has it
        for (const Member& m : members_)
            if (m.id != config_.id && matchIndex_[m.id] >= n) ++replicas;
        if (replicas >= quorum()) {
            commitIndex_ = n;  // also commits everything before n
            break;
        }
    }

    // A leader that removed itself from the configuration steps down once
    // that change has committed.
    if (role_ == Role::Leader && !isMember() && commitIndex_ >= configIndex_) {
        becomeFollower(term_, std::nullopt);
        return;
    }
    processReads();
}

void RaftNode::processReads() {
    if (role_ != Role::Leader) return;
    while (!reads_.empty()) {
        PendingRead& read = reads_.front();

        // Until something from our own term has committed, our commit index
        // may be behind the cluster's, and a read would be stale.
        if (commitIndex_ < termStartIndex_) return;
        if (!read.index) read.index = commitIndex_;

        std::size_t confirmed = isMember() ? 1 : 0;
        for (const Member& m : members_)
            if (m.id != config_.id && ackedSeq_[m.id] >= read.seq) ++confirmed;
        if (confirmed < quorum()) return;

        readStates_.push_back(ReadState{read.id, *read.index});
        reads_.pop_front();
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

Members RaftNode::initialMembers() const {
    Members out;
    for (NodeId id : config_.peers) {
        auto addr = config_.addresses.find(id);
        out.push_back(Member{id, addr == config_.addresses.end() ? std::string() : addr->second});
    }
    out = sortedMembers(std::move(out));
    out.erase(std::unique(out.begin(), out.end(), [](const Member& a, const Member& b) { return a.id == b.id; }),
              out.end());
    return out;
}

bool RaftNode::isMemberId(NodeId id) const {
    return std::any_of(members_.begin(), members_.end(), [id](const Member& m) { return m.id == id; });
}

// The configuration is defined by the log: the newest Config entry wins,
// whether or not it has committed. Without one, the snapshot's (or the
// initial) configuration applies.
void RaftNode::recomputeConfig() {
    members_ = snapshot_ ? snapshot_->members : initial_;
    configIndex_ = snapshot_ ? snapshot_->index : 0;
    for (Index i = log_.firstIndex(); i <= log_.lastIndex(); ++i) {
        const Entry& e = log_.at(i);
        Members decoded;
        if (e.type == Entry::Type::Config && decodeMembers(e.data, decoded)) {
            members_ = std::move(decoded);
            configIndex_ = i;
        }
    }
    if (role_ == Role::Leader) syncReplicationState();
}

void RaftNode::applyConfig(Members members, Index index) {
    members_ = std::move(members);
    configIndex_ = index;
    if (role_ == Role::Leader) syncReplicationState();
}

// Keeps the leader's per-follower bookkeeping in step with the configuration:
// new members start being tracked, removed ones are forgotten.
void RaftNode::syncReplicationState() {
    for (const Member& m : members_) {
        if (m.id == config_.id || nextIndex_.count(m.id)) continue;
        nextIndex_[m.id] = log_.lastIndex() + 1;
        matchIndex_[m.id] = 0;
    }
    for (auto it = nextIndex_.begin(); it != nextIndex_.end();) {
        if (isMemberId(it->first)) {
            ++it;
            continue;
        }
        matchIndex_.erase(it->first);
        ackedSeq_.erase(it->first);
        snapOffset_.erase(it->first);
        snapIdle_.erase(it->first);
        activeSinceCheck_.erase(it->first);
        it = nextIndex_.erase(it);
    }
}

// The configuration as of log index `index`: used to record it in a snapshot.
Members RaftNode::membersAt(Index index) const {
    Members result = snapshot_ ? snapshot_->members : initial_;
    for (Index i = log_.firstIndex(); i <= std::min(index, log_.lastIndex()); ++i) {
        const Entry& e = log_.at(i);
        Members decoded;
        if (e.type == Entry::Type::Config && decodeMembers(e.data, decoded)) result = std::move(decoded);
    }
    return result;
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
