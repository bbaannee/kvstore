#pragma once

#include <variant>
#include <vector>

#include "raft/types.h"

namespace kv::raft {

// "Vote for me." Sent by a candidate to every other member.
struct RequestVote {
    Index lastLogIndex = 0;  // so voters can check the candidate's log is
    Term lastLogTerm = 0;    // at least as up-to-date as their own

    bool operator==(const RequestVote&) const = default;
};

struct RequestVoteResponse {
    bool granted = false;

    bool operator==(const RequestVoteResponse&) const = default;
};

// Sent by the leader to replicate entries. With no entries it's a heartbeat.
struct AppendEntries {
    Index prevLogIndex = 0;      // the entry just before `entries`...
    Term prevLogTerm = 0;        // ...and its term; the follower must have it
    std::vector<Entry> entries;  // may be empty (heartbeat)
    Index leaderCommit = 0;      // so followers learn what is committed

    // Round number, echoed back in the response. A linearizable read waits
    // until a majority has answered a round that started after the read
    // arrived: that proves we were still the leader when it arrived.
    std::uint64_t seq = 0;

    bool operator==(const AppendEntries&) const = default;
};

struct AppendEntriesResponse {
    bool success = false;
    Index matchIndex = 0;  // on success: follower's log matches the leader's up to here
    Index rejectHint = 0;  // on failure: where the leader should look for agreement next
    std::uint64_t seq = 0; // copied from the AppendEntries this answers

    bool operator==(const AppendEntriesResponse&) const = default;
};

// One piece of a snapshot, sent to a follower that is so far behind that the
// entries it needs have been compacted away. A snapshot can be big, so it
// travels in chunks: offset 0 starts a transfer, `done` marks the last chunk.
struct InstallSnapshot {
    Index lastIndex = 0;  // the snapshot's index, term and configuration;
    Term lastTerm = 0;    // repeated in every chunk so a chunk can be checked on its own
    Members members;
    std::uint64_t offset = 0;  // where `data` belongs in the whole snapshot
    std::string data;
    bool done = false;

    bool operator==(const InstallSnapshot&) const = default;
};

struct InstallSnapshotResponse {
    Index lastIndex = 0;            // which snapshot this answers
    std::uint64_t nextOffset = 0;   // the offset the follower wants next
    bool done = false;              // the follower has installed the whole snapshot (or already had it)

    bool operator==(const InstallSnapshotResponse&) const = default;
};

// Envelope around every message. `term` is the sender's current term, the
// one field every Raft message has.
struct Message {
    NodeId from = 0;
    NodeId to = 0;
    Term term = 0;
    std::variant<RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse, InstallSnapshot,
                 InstallSnapshotResponse>
        body;

    bool operator==(const Message&) const = default;
};

}  // namespace kv::raft
