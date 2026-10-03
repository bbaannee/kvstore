// Leadership details in RaftNode: linearizable reads (ReadIndex), a leader
// stepping down when it can't reach a majority, and leader stickiness.

#include <gtest/gtest.h>

#include "node_test_util.h"

using namespace kv::test;

// ---------------------------------------------------------------------------
// ReadIndex
// ---------------------------------------------------------------------------

TEST(RaftNodeReads, ASingleNodeConfirmsAReadAtOnce) {
    RaftNode n(config(1, {1}), {}, {});
    tickUntilCandidate(n);
    n.takeReady();

    auto id = n.requestRead();
    ASSERT_TRUE(id);
    Ready rd = n.takeReady();
    ASSERT_EQ(rd.readStates.size(), 1u);
    EXPECT_EQ(rd.readStates[0].id, *id);
    EXPECT_EQ(rd.readStates[0].index, n.commitIndex());
}

TEST(RaftNodeReads, OnlyALeaderTakesReads) {
    RaftNode follower(config(1), {}, {});
    EXPECT_FALSE(follower.requestRead());

    RaftNode candidate(config(1), {}, {});
    tickUntilCandidate(candidate);
    EXPECT_FALSE(candidate.requestRead());
}

// A read is answered only after a majority has confirmed a heartbeat round that began AFTER the read arrived.
TEST(RaftNodeReads, AReadWaitsForAMajorityToAnswerALaterHeartbeat) {
    RaftNode n = leaderOfThree();
    ack(n, 2, 1);  // commits the no-op, so the leader knows its commit index
    n.takeReady();

    auto id = n.requestRead();
    ASSERT_TRUE(id);
    Ready rd = n.takeReady();
    EXPECT_TRUE(rd.readStates.empty());
    const std::uint64_t seq = bodyTo<AppendEntries>(rd, 2).seq;  // the round this read is waiting on
    EXPECT_EQ(bodyTo<AppendEntries>(rd, 3).seq, seq);
    EXPECT_GT(seq, 0u);

    // An answer to an EARLIER heartbeat proves nothing about leadership at the time of the read.
    ack(n, 2, 1, seq - 1);
    EXPECT_TRUE(n.takeReady().readStates.empty());

    // An answer to this round: with the leader's own vote that's a majority of 3.
    ack(n, 3, 1, seq);
    rd = n.takeReady();
    ASSERT_EQ(rd.readStates.size(), 1u);
    EXPECT_EQ(rd.readStates[0].id, *id);
    EXPECT_EQ(rd.readStates[0].index, 1u);
}

TEST(RaftNodeReads, ReadsAreConfirmedInTheOrderTheyArrived) {
    RaftNode n = leaderOfThree();
    ack(n, 2, 1);
    n.takeReady();

    auto first = n.requestRead();
    n.takeReady();
    n.propose("write between the reads");
    ack(n, 2, 2);
    n.takeReady();
    auto second = n.requestRead();
    Ready rd = n.takeReady();
    const std::uint64_t secondSeq = bodyTo<AppendEntries>(rd, 3).seq;

    // One answer that covers both rounds confirms both, oldest first, each at the commit index of its own time.
    ack(n, 3, 2, secondSeq);
    rd = n.takeReady();
    ASSERT_EQ(rd.readStates.size(), 2u);
    EXPECT_EQ(rd.readStates[0].id, *first);
    EXPECT_EQ(rd.readStates[1].id, *second);
    EXPECT_LE(rd.readStates[0].index, rd.readStates[1].index);
}

// A new leader's commit index may lag behind the cluster's until something from its own term commits.
TEST(RaftNodeReads, ANewLeaderAnswersOnlyAfterCommittingInItsOwnTerm) {
    RaftNode n = leaderOfThree();  // its no-op (index 1) hasn't committed
    auto id = n.requestRead();
    ASSERT_TRUE(id);
    const std::uint64_t seq = bodyTo<AppendEntries>(n.takeReady(), 2).seq;

    // Node 2 confirms leadership but hasn't stored the no-op: nothing has committed yet.
    ack(n, 2, 0, seq);
    EXPECT_TRUE(n.takeReady().readStates.empty());

    // Once the no-op commits, the read can go.
    ack(n, 3, 1, seq);
    Ready rd = n.takeReady();
    ASSERT_EQ(rd.readStates.size(), 1u);
    EXPECT_EQ(rd.readStates[0].index, 1u);
}

TEST(RaftNodeReads, ALeaderThatIsDeposedNeverConfirmsItsPendingReads) {
    RaftNode n = leaderOfThree();
    ack(n, 2, 1);
    n.takeReady();
    ASSERT_TRUE(n.requestRead());
    n.takeReady();

    n.step(msg(3, 1, n.term() + 5, AppendEntriesResponse{false, 0, 0, 0}));  // a newer term exists
    ASSERT_EQ(n.role(), Role::Follower);
    EXPECT_TRUE(n.takeReady().readStates.empty());
}

// ---------------------------------------------------------------------------
// Check-quorum: a leader that can't reach a majority steps down
// ---------------------------------------------------------------------------

TEST(RaftNodeLeadership, ALeaderThatHearsFromNobodyStepsDown) {
    RaftNode n = leaderOfThree();  // election timeout: 10 ticks
    for (int i = 0; i < 9; ++i) n.tick();
    EXPECT_EQ(n.role(), Role::Leader);
    n.tick();
    EXPECT_EQ(n.role(), Role::Follower) << "it can't reach a majority, so a new leader may exist elsewhere";
}

TEST(RaftNodeLeadership, ALeaderThatKeepsHearingFromAMajorityStays) {
    RaftNode n = leaderOfThree();
    for (int i = 1; i <= 100; ++i) {
        n.tick();
        if (i % 3 == 0) ack(n, 2, 1);  // one follower answers each heartbeat: with ourselves, a majority of 3
        n.takeReady();
    }
    EXPECT_EQ(n.role(), Role::Leader);
}

TEST(RaftNodeLeadership, ASingleNodeLeaderNeverStepsDown) {
    RaftNode n(config(1, {1}), {}, {});
    tickUntilCandidate(n);
    for (int i = 0; i < 200; ++i) n.tick();
    EXPECT_EQ(n.role(), Role::Leader);
}

// ---------------------------------------------------------------------------
// Leader stickiness
// ---------------------------------------------------------------------------

// A server that keeps timing out (cut off, or removed) must not be able to
// depose a healthy leader just by announcing a higher term.
TEST(RaftNodeLeadership, AFollowerThatRecentlyHeardTheLeaderIgnoresRequestsForANewTerm) {
    RaftNode n(config(1), {}, {});
    n.step(msg(2, 1, 1, AppendEntries{0, 0, {}, 0}));  // the leader's heartbeat
    n.takeReady();
    ASSERT_EQ(n.leader(), 2u);

    n.step(msg(3, 1, 2, RequestVote{0, 0}));
    EXPECT_EQ(n.term(), 1u) << "the vote request must not even raise our term";
    EXPECT_TRUE(n.takeReady().messages.empty());

    // After half an election timeout of silence the leader is in doubt, and a real election can proceed.
    for (int i = 0; i < 5; ++i) n.tick();
    n.step(msg(3, 1, 2, RequestVote{0, 0}));
    EXPECT_EQ(n.term(), 2u);
    EXPECT_TRUE(std::get<RequestVoteResponse>(n.takeReady().messages.at(0).body).granted);
}

TEST(RaftNodeLeadership, ASittingLeaderIgnoresRequestsForANewTerm) {
    RaftNode n = leaderOfThree();
    n.step(msg(2, 1, 99, RequestVote{50, 50}));
    EXPECT_EQ(n.role(), Role::Leader);
    EXPECT_EQ(n.term(), 1u);
    EXPECT_TRUE(n.takeReady().messages.empty());
}

TEST(RaftNodeLeadership, MessagesFromANewerLeaderAreStillAccepted) {
    // Stickiness only applies to vote requests: a real new leader's AppendEntries must get through.
    RaftNode n = leaderOfThree();
    n.step(msg(2, 1, 5, AppendEntries{0, 0, {}, 0}));
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.term(), 5u);
    EXPECT_EQ(n.leader(), 2u);
}
