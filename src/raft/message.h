#pragma once

#include <variant>
#include <vector>

#include "raft/types.h"

namespace kv::raft {

// "Vote for me." Sent by a candidate to every other node.
struct RequestVote {
    Index lastLogIndex = 0;  // so voters can check the candidate's log is
    Term lastLogTerm = 0;    // at least as up-to-date as their own
};

struct RequestVoteResponse {
    bool granted = false;
};

// Sent by the leader to replicate entries. With no entries it's a heartbeat.
struct AppendEntries {
    Index prevLogIndex = 0;      // the entry just before `entries`...
    Term prevLogTerm = 0;        // ...and its term; the follower must have it
    std::vector<Entry> entries;  // may be empty (heartbeat)
    Index leaderCommit = 0;      // so followers learn what is committed
};

struct AppendEntriesResponse {
    bool success = false;
    Index matchIndex = 0;  // on success: follower's log matches the leader's up to here
    Index rejectHint = 0;  // on failure: where the leader should look for agreement next
};

// Envelope around every message. `term` is the sender's current term, the
// one field every Raft message has.
struct Message {
    NodeId from = 0;
    NodeId to = 0;
    Term term = 0;
    std::variant<RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse> body;
};

}  // namespace kv::raft
