#pragma once

// Helpers for tests that drive a single RaftNode by hand with hand-made messages.

#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "raft/raft_node.h"

namespace kv::test {

using namespace kv::raft;

inline Config config(NodeId id, std::vector<NodeId> peers = {1, 2, 3}) {
    Config c;
    c.id = id;
    c.peers = std::move(peers);
    return c;
}

inline Entry entry(Term term, Index index, std::string data = "") {
    return Entry{term, index, Entry::Type::Command, std::move(data)};
}

template <class T>
Message msg(NodeId from, NodeId to, Term term, T body) {
    return Message{from, to, term, std::move(body)};
}

inline void tickUntilCandidate(RaftNode& n, int maxTicks = 1000) {
    for (int i = 0; i < maxTicks && n.role() != Role::Candidate && n.role() != Role::Leader; ++i) n.tick();
    ASSERT_TRUE(n.role() == Role::Candidate || n.role() == Role::Leader);
}

// Node `cfg.id` (default: 1 of {1, 2, 3}), elected leader of term 1 by the vote of node 2,
// with its output so far taken. Its log is: [1: no-op].
inline RaftNode leaderWith(Config cfg) {
    RaftNode n(std::move(cfg), {}, {});
    tickUntilCandidate(n);
    if (n.role() != Role::Leader) n.step(msg(2, n.id(), n.term(), RequestVoteResponse{true}));
    n.takeReady();
    return n;
}

inline RaftNode leaderOfThree() { return leaderWith(config(1)); }

// The first message of type T in `rd` addressed to `peer`.
template <class T>
const T& bodyTo(const Ready& rd, NodeId peer) {
    for (const auto& m : rd.messages)
        if (m.to == peer)
            if (const T* body = std::get_if<T>(&m.body)) return *body;
    throw std::logic_error("no such message to that peer");
}

template <class T>
bool hasMessageTo(const Ready& rd, NodeId peer) {
    for (const auto& m : rd.messages)
        if (m.to == peer && std::holds_alternative<T>(m.body)) return true;
    return false;
}

// `peer` tells the leader that its log matches up to `matchIndex`.
inline void ack(RaftNode& leader, NodeId peer, Index matchIndex, std::uint64_t seq = 0) {
    leader.step(msg(peer, leader.id(), leader.term(), AppendEntriesResponse{true, matchIndex, 0, seq}));
}

}  // namespace kv::test
