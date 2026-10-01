// Unit tests for individual Raft rules, driving one RaftNode by hand.

#include "raft/raft_node.h"

#include <gtest/gtest.h>

using namespace kv::raft;

namespace {

Config config(NodeId id, std::vector<NodeId> peers = {1, 2, 3}) {
    Config c;
    c.id = id;
    c.peers = std::move(peers);
    return c;
}

RaftNode freshNode(NodeId id, std::vector<NodeId> peers = {1, 2, 3}) {
    return RaftNode(config(id, std::move(peers)), {}, {});
}

void tickUntilCandidate(RaftNode& n) {
    for (int i = 0; i < 100 && n.role() != Role::Candidate; ++i) n.tick();
    ASSERT_EQ(n.role(), Role::Candidate);
}

Entry entry(Term term, Index index) { return Entry{term, index, Entry::Type::Command, {}}; }

template <class T>
Message msg(NodeId from, NodeId to, Term term, T body) {
    return Message{from, to, term, std::move(body)};
}

}  // namespace

TEST(RaftNode, StartsAsFollower) {
    RaftNode n = freshNode(1);
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.term(), 0u);
    EXPECT_FALSE(n.leader());
    EXPECT_FALSE(n.propose("x"));  // only leaders accept proposals
}

TEST(RaftNode, SingleNodeElectsItselfAndCommits) {
    RaftNode n = freshNode(1, {1});
    for (int i = 0; i < 100 && n.role() != Role::Leader; ++i) n.tick();
    ASSERT_EQ(n.role(), Role::Leader);

    auto index = n.propose("hello");
    ASSERT_TRUE(index);
    Ready rd = n.takeReady();
    ASSERT_EQ(rd.committedEntries.size(), 2u);  // the leader's no-op, then our command
    EXPECT_EQ(rd.committedEntries[0].type, Entry::Type::NoOp);
    EXPECT_EQ(rd.committedEntries[1].data, "hello");
    EXPECT_EQ(rd.committedEntries[1].index, *index);
}

TEST(RaftNode, TimeoutStartsElection) {
    RaftNode n = freshNode(1);
    tickUntilCandidate(n);
    EXPECT_EQ(n.term(), 1u);

    Ready rd = n.takeReady();
    ASSERT_TRUE(rd.hardState);
    EXPECT_EQ(rd.hardState->term, 1u);
    EXPECT_EQ(rd.hardState->votedFor, 1u);  // voted for itself, and that must be persisted
    ASSERT_EQ(rd.messages.size(), 2u);      // RequestVote to nodes 2 and 3
    for (const auto& m : rd.messages) EXPECT_TRUE(std::holds_alternative<RequestVote>(m.body));
}

TEST(RaftNode, WinsWithMajorityOfVotes) {
    RaftNode n = freshNode(1);
    tickUntilCandidate(n);
    n.takeReady();
    n.step(msg(2, 1, 1, RequestVoteResponse{true}));  // self + node 2 = 2 of 3
    EXPECT_EQ(n.role(), Role::Leader);
    EXPECT_EQ(n.leader(), 1u);
    EXPECT_EQ(n.log().at(1).type, Entry::Type::NoOp);  // new leaders append a no-op
}

TEST(RaftNode, VotesAtMostOncePerTerm) {
    RaftNode n = freshNode(1);
    n.step(msg(2, 1, 1, RequestVote{0, 0}));
    n.step(msg(3, 1, 1, RequestVote{0, 0}));
    Ready rd = n.takeReady();
    ASSERT_EQ(rd.messages.size(), 2u);
    EXPECT_TRUE(std::get<RequestVoteResponse>(rd.messages[0].body).granted);
    EXPECT_FALSE(std::get<RequestVoteResponse>(rd.messages[1].body).granted);
    EXPECT_EQ(rd.hardState->votedFor, 2u);
}

TEST(RaftNode, RefusesVoteToCandidateWithOlderLog) {
    // Our log has an entry from term 2; the candidate's last entry is term 1.
    RaftNode n(config(1), {2, std::nullopt}, {entry(1, 1), entry(2, 2)});
    n.step(msg(2, 1, 3, RequestVote{5, 1}));  // longer log, but older last term
    Ready rd = n.takeReady();
    EXPECT_FALSE(std::get<RequestVoteResponse>(rd.messages.at(0).body).granted);

    n.step(msg(3, 1, 3, RequestVote{2, 2}));  // same last term, same length: OK
    rd = n.takeReady();
    EXPECT_TRUE(std::get<RequestVoteResponse>(rd.messages.at(0).body).granted);
}

TEST(RaftNode, StepsDownOnHigherTerm) {
    RaftNode n = freshNode(1);
    tickUntilCandidate(n);
    n.step(msg(2, 1, 1, RequestVoteResponse{true}));
    ASSERT_EQ(n.role(), Role::Leader);

    n.step(msg(3, 1, 7, AppendEntriesResponse{false, 0, 0}));
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.term(), 7u);
}

TEST(RaftNode, RejectsStaleTermAndTellsSenderTheNewTerm) {
    RaftNode n(config(1), {5, std::nullopt}, {});
    n.step(msg(2, 1, 3, AppendEntries{0, 0, {}, 0}));
    Ready rd = n.takeReady();
    ASSERT_EQ(rd.messages.size(), 1u);
    EXPECT_EQ(rd.messages[0].term, 5u);
    EXPECT_FALSE(std::get<AppendEntriesResponse>(rd.messages[0].body).success);
}

TEST(RaftNode, RejectsAppendWhenPreviousEntryIsMissing) {
    RaftNode n = freshNode(1);
    n.step(msg(2, 1, 1, AppendEntries{5, 1, {entry(1, 6)}, 0}));  // we have no entry 5
    Ready rd = n.takeReady();
    const auto& resp = std::get<AppendEntriesResponse>(rd.messages.at(0).body);
    EXPECT_FALSE(resp.success);
    EXPECT_EQ(resp.rejectHint, 0u);  // our log is empty: start from the beginning
    EXPECT_EQ(n.log().lastIndex(), 0u);
}

TEST(RaftNode, OverwritesConflictingUncommittedEntries) {
    // Entries 2 and 3 came from a term-1 leader that never committed them.
    RaftNode n(config(1), {1, std::nullopt}, {entry(1, 1), entry(1, 2), entry(1, 3)});
    n.step(msg(2, 1, 2, AppendEntries{1, 1, {Entry{2, 2, Entry::Type::Command, "new"}}, 2}));

    EXPECT_EQ(n.log().lastIndex(), 2u);  // entry 3 is gone
    EXPECT_EQ(n.log().at(2).term, 2u);
    EXPECT_EQ(n.commitIndex(), 2u);

    Ready rd = n.takeReady();
    ASSERT_EQ(rd.entries.size(), 1u);  // only the replaced entry needs persisting
    EXPECT_EQ(rd.entries[0].index, 2u);
    EXPECT_TRUE(std::get<AppendEntriesResponse>(rd.messages.at(0).body).success);
}

TEST(RaftNode, DuplicateAppendDoesNotTruncate) {
    RaftNode n(config(1), {1, std::nullopt}, {entry(1, 1), entry(1, 2), entry(1, 3)});
    // An old, delayed message carrying only entry 2 must not drop entry 3.
    n.step(msg(2, 1, 1, AppendEntries{1, 1, {entry(1, 2)}, 0}));
    EXPECT_EQ(n.log().lastIndex(), 3u);
}

// Figure 8 in the Raft paper: a leader must not commit an entry from an
// older term just because it's on a majority. A future leader could still
// overwrite it. It becomes committed only together with a current-term entry.
TEST(RaftNode, DoesNotCommitOldTermEntryByCountingReplicas) {
    // Entry 2 (term 2) was left behind by an older leader.
    RaftNode n(config(1), {3, std::nullopt}, {entry(1, 1), entry(2, 2)});
    tickUntilCandidate(n);                             // term 4
    n.step(msg(2, 1, 4, RequestVoteResponse{true}));
    ASSERT_EQ(n.role(), Role::Leader);                 // appended no-op at index 3
    ASSERT_EQ(n.log().lastIndex(), 3u);

    // Entry 2 reaches a majority (nodes 1, 2, 3), the no-op doesn't yet.
    n.step(msg(2, 1, 4, AppendEntriesResponse{true, 2, 0}));
    n.step(msg(3, 1, 4, AppendEntriesResponse{true, 2, 0}));
    EXPECT_EQ(n.commitIndex(), 0u);

    // Once the current-term no-op is on a majority, both commit together.
    n.step(msg(2, 1, 4, AppendEntriesResponse{true, 3, 0}));
    EXPECT_EQ(n.commitIndex(), 3u);
}

TEST(RaftNode, RestartKeepsTermVoteAndLog) {
    RaftNode n(config(1), {4, 2u}, {entry(3, 1)});
    EXPECT_EQ(n.term(), 4u);
    EXPECT_EQ(n.log().lastIndex(), 1u);
    n.step(msg(3, 1, 4, RequestVote{9, 9}));  // already voted for 2 in term 4
    Ready rd = n.takeReady();
    EXPECT_FALSE(std::get<RequestVoteResponse>(rd.messages.at(0).body).granted);
}
