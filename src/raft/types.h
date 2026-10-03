#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace kv::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;   // election epoch; only ever increases
using Index = std::uint64_t;  // position in the log; the first entry is 1, 0 means "before the log"

// One server of the cluster, and where to reach it.
struct Member {
    NodeId id = 0;
    std::string address;  // "host:port"; empty if unknown (tests that have no network)

    bool operator==(const Member&) const = default;
};

// A cluster configuration: the members that vote and replicate. Always
// sorted by id with no duplicates, so equal configurations compare equal.
using Members = std::vector<Member>;

// One entry in the replicated log.
struct Entry {
    enum class Type : std::uint8_t {
        Command,  // a client command for the state machine (e.g. an encoded PUT)
        NoOp,     // appended by every new leader; see RaftNode::becomeLeader()
        Config,   // a new cluster configuration (data = encoded Members); see RaftNode::proposeConfigChange()
    };

    Term term = 0;    // term of the leader that created this entry
    Index index = 0;  // position in the log
    Type type = Type::Command;
    std::string data;

    bool operator==(const Entry&) const = default;
};

// State that must be on disk before the node sends any message that
// depends on it. If a node forgot its vote after a crash, it could vote
// twice in one term and two leaders could be elected.
struct HardState {
    Term term = 0;
    std::optional<NodeId> votedFor;  // who we voted for in `term`, if anyone

    bool operator==(const HardState&) const = default;
};

// The state machine's state as of log index `index`, standing in for every
// log entry up to and including it. Once a snapshot exists, those entries
// can be thrown away: that is what keeps the log (and restart time) bounded.
struct Snapshot {
    Index index = 0;  // the last log entry the snapshot covers
    Term term = 0;    // that entry's term (needed for the AppendEntries consistency check)
    Members members;  // the cluster configuration as of `index`
    std::string data; // the serialized state machine (opaque to Raft)

    bool operator==(const Snapshot&) const = default;
};

}  // namespace kv::raft
