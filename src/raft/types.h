#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace kv::raft {

using NodeId = std::uint64_t;
using Term = std::uint64_t;   // election epoch; only ever increases
using Index = std::uint64_t;  // position in the log; the first entry is 1, 0 means "before the log"

// One entry in the replicated log.
struct Entry {
    enum class Type : std::uint8_t {
        Command,  // a client command for the state machine (e.g. an encoded PUT)
        NoOp,     // appended by every new leader; see RaftNode::becomeLeader()
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

}  // namespace kv::raft
