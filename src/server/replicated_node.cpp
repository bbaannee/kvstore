#include "server/replicated_node.h"

#include <filesystem>
#include <functional>
#include <utility>

#include "storage/log_reader.h"

namespace kv::server {

namespace {

std::string raftLogPath(const std::string& dir) {
    std::filesystem::create_directories(dir);
    return (std::filesystem::path(dir) / "raft.log").string();
}

std::unique_ptr<KvStorage> makeStorage(const std::string& dir, const ReplicatedNode::Options& options) {
    if (options.engine == ReplicatedNode::Engine::Memory) return std::make_unique<MemoryStorage>();
    lsm::Options lsmOptions = options.lsm;
    lsmOptions.dir = (std::filesystem::path(dir) / "state").string();
    // The Raft log is what makes writes durable. The state machine only has to be durable up to
    // the point where Raft throws log entries away, so it syncs then (see maybeSnapshot), not on
    // every write.
    lsmOptions.syncWal = false;
    lsmOptions.syncTables = options.syncOnAppend;
    return std::make_unique<LsmStorage>(std::move(lsmOptions));
}

// Brings the state machine to the point where Raft resumes, and returns that point.
//   memory engine: restored from the newest snapshot (or empty)
//   LSM engine:    it already has its own state; only if it is BEHIND the snapshot is it restored from it
raft::Index prepareStateMachine(KvStateMachine& state, const raft::RaftState& recovered, const std::string& dir) {
    raft::Index applied = state.appliedIndex();
    if (recovered.snapshot && applied < recovered.snapshot->index) {
        if (!state.restore(recovered.snapshot->data, recovered.snapshot->index))
            throw storage::CorruptLogError(dir + ": the stored snapshot does not contain a valid state");
        applied = recovered.snapshot->index;
    }
    const raft::Index lastInLog = (recovered.snapshot ? recovered.snapshot->index : 0) + recovered.log.size();
    if (applied > lastInLog)
        throw storage::CorruptLogError(dir + ": the state machine has applied entries that are missing from the Raft log");
    return applied;
}

}  // namespace

ReplicatedNode::ReplicatedNode(const std::string& dir, raft::Config config, Options options)
    : options_(options),
      storage_(raftLogPath(dir), options.syncOnAppend),
      state_(options.maxSessions, makeStorage(dir, options)),
      appliedIndex_(prepareStateMachine(state_, storage_.recovery().state, dir)),
      raft_(std::move(config), storage_.recovery().state.hardState, storage_.recovery().state.log,
            storage_.recovery().state.snapshot, appliedIndex_) {}

void ReplicatedNode::tick() {
    raft_.tick();
    maybeProcessReady();
}

void ReplicatedNode::step(const raft::Message& msg) {
    raft_.step(msg);
    maybeProcessReady();
}

void ReplicatedNode::submit(const Command& command, Callback callback) {
    // Remember the term first: propose() stamps the entry with it.
    const raft::Term term = raft_.term();
    auto index = raft_.propose(encodeCommand(command));
    if (!index) {
        callback(RequestStatus::NotLeader, {});
        return;
    }
    pending_[*index] = Pending{term, std::move(callback)};
    maybeProcessReady();  // may commit right away (single-node cluster)
}

void ReplicatedNode::read(const std::string& key, Callback callback) {
    const raft::Term term = raft_.term();
    auto id = raft_.requestRead();
    if (!id) {
        callback(RequestStatus::NotLeader, {});
        return;
    }
    unconfirmedReads_[*id] = PendingRead{key, term, std::move(callback)};
    maybeProcessReady();  // may confirm right away (single-node cluster)
}

void ReplicatedNode::changeMembership(raft::Members newMembers, Callback callback) {
    const raft::Term term = raft_.term();
    raft::ConfigError error = raft::ConfigError::None;
    auto index = raft_.proposeConfigChange(std::move(newMembers), &error);
    if (!index) {
        callback(error == raft::ConfigError::NotLeader ? RequestStatus::NotLeader : RequestStatus::Rejected, {});
        return;
    }
    pending_[*index] = Pending{term, std::move(callback)};
    maybeProcessReady();
}

bool ReplicatedNode::snapshotNow() {
    state_.sync();  // Raft is about to discard log entries: what they did must be durable in the state machine first
    const bool taken = raft_.compact(appliedIndex_, state_.serialize());
    processReady();
    return taken;
}

void ReplicatedNode::processReady() {
    // A snapshot taken at the end of one round produces output for the next.
    while (raft_.hasReady()) {
        raft::Ready rd = raft_.takeReady();

        // 1. Persist first. Messages and answers below may promise things
        //    ("I voted for you", "I have entry 7") that must survive a crash.
        storage_.save({rd.hardState, std::move(rd.entries), rd.snapshot});

        // 2. Queue messages for the network.
        for (auto& m : rd.messages) outgoing_.push_back(std::move(m));

        // 3. A snapshot from the leader replaces our state wholesale.
        if (rd.snapshot && rd.snapshotFromLeader) {
            if (!state_.restore(rd.snapshot->data, rd.snapshot->index))
                throw storage::CorruptLogError("received a snapshot that does not contain a valid state");
            appliedIndex_ = rd.snapshot->index;

            // Requests we were waiting on are now covered by the snapshot, and we
            // can't tell whether OUR entry survived into it. Answering "aborted"
            // would be a claim we can't back up, so we say nothing: the client will
            // time out and retry, and its sequence number makes that safe.
            pending_.erase(pending_.begin(), pending_.upper_bound(rd.snapshot->index));
        }

        // 4. Apply committed entries and answer the clients waiting on them. All the entries of
        //    this round go to the storage as ONE batch (one write-ahead-log record), and the
        //    answers are given after it, so a callback never runs in the middle of a batch.
        std::vector<std::function<void()>> answers;
        state_.beginBatch();
        for (const raft::Entry& entry : rd.committedEntries) {
            ApplyResult result = state_.apply(entry);
            appliedIndex_ = entry.index;

            auto it = pending_.find(entry.index);
            if (it == pending_.end()) continue;
            Pending p = std::move(it->second);
            pending_.erase(it);

            // Same index but a different term means our entry was overwritten.
            // Answering "committed" here would be a lie.
            const bool ours = entry.term == p.term;
            answers.push_back([p = std::move(p), ours, result = std::move(result)]() mutable {
                if (ours) p.callback(RequestStatus::Committed, result);
                else p.callback(RequestStatus::Aborted, {});
            });
        }
        state_.endBatch();
        for (auto& answer : answers) answer();

        // 5. Reads that Raft has confirmed wait for the state machine to catch up.
        for (const raft::ReadState& rs : rd.readStates) {
            auto it = unconfirmedReads_.find(rs.id);
            if (it == unconfirmedReads_.end()) continue;
            confirmedReads_.emplace(rs.index, std::move(it->second));
            unconfirmedReads_.erase(it);
        }
        serveReadyReads();

        maybeSnapshot();
    }
    failUnconfirmedReads();
}

// Answers confirmed reads whose index has been applied.
void ReplicatedNode::serveReadyReads() {
    while (!confirmedReads_.empty() && confirmedReads_.begin()->first <= appliedIndex_) {
        PendingRead r = std::move(confirmedReads_.begin()->second);
        confirmedReads_.erase(confirmedReads_.begin());
        r.callback(RequestStatus::Committed, state_.read(r.key));
    }
}

// Reads that were never confirmed, because leadership was lost first, can't be
// answered: we don't know whether a newer leader has newer data. They didn't
// happen as far as the cluster is concerned; the client tries another node.
void ReplicatedNode::failUnconfirmedReads() {
    const bool leading = raft_.role() == raft::Role::Leader;
    for (auto it = unconfirmedReads_.begin(); it != unconfirmedReads_.end();) {
        if (leading && it->second.term == raft_.term()) {
            ++it;
            continue;
        }
        PendingRead r = std::move(it->second);
        it = unconfirmedReads_.erase(it);
        r.callback(RequestStatus::NotLeader, {});
    }
}

void ReplicatedNode::maybeSnapshot() {
    if (options_.snapshotThreshold == 0) return;
    if (appliedIndex_ < raft_.snapshotIndex() + options_.snapshotThreshold) return;
    state_.sync();  // see snapshotNow()
    raft_.compact(appliedIndex_, state_.serialize());  // its output is handled by the next round of the loop
}

}  // namespace kv::server
