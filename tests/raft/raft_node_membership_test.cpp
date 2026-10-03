// Cluster membership in RaftNode: the bootstrap configuration, joining
// servers, and adding or removing one server at a time.

#include <gtest/gtest.h>

#include "node_test_util.h"
#include "raft/members_codec.h"

using namespace kv::test;

namespace {

Members ids(std::initializer_list<NodeId> list) {
    Members m;
    for (NodeId id : list) m.push_back(Member{id, ""});
    return m;
}

Entry configEntry(Term term, Index index, const Members& m) {
    return Entry{term, index, Entry::Type::Config, encodeMembers(m)};
}

// A 3-node leader that has committed its no-op, so it may change the configuration.
RaftNode readyLeader() {
    RaftNode n = leaderOfThree();
    ack(n, 2, 1);
    n.takeReady();
    return n;
}

}  // namespace

TEST(RaftNodeMembership, AFreshClusterRecordsItsConfigurationAsEntryOne) {
    Config c = config(1);
    c.bootstrapConfigEntry = true;
    c.addresses = {{1, "a:1"}, {2, "b:2"}, {3, "c:3"}};
    RaftNode n(c, {}, {});

    ASSERT_EQ(n.log().lastIndex(), 1u);
    EXPECT_EQ(n.log().at(1).type, Entry::Type::Config);
    EXPECT_EQ(n.log().at(1).term, 0u);  // before any election, so identical on every node
    Members decoded;
    ASSERT_TRUE(decodeMembers(n.log().at(1).data, decoded));
    EXPECT_EQ(decoded, (Members{{1, "a:1"}, {2, "b:2"}, {3, "c:3"}}));
    EXPECT_EQ(n.members(), decoded);
    EXPECT_EQ(n.configIndex(), 1u);

    Ready rd = n.takeReady();
    ASSERT_EQ(rd.entries.size(), 1u);  // it must be persisted like any other entry

    // After a restart the entry is already in the log; nothing is added a second time.
    RaftNode again(c, {0, std::nullopt}, {configEntry(0, 1, decoded)});
    EXPECT_EQ(again.log().lastIndex(), 1u);
    EXPECT_FALSE(again.hasReady());
}

TEST(RaftNodeMembership, WithoutTheBootstrapOptionNoEntryIsWritten) {
    RaftNode n(config(1), {}, {});
    EXPECT_EQ(n.log().lastIndex(), 0u);
    EXPECT_EQ(n.members(), ids({1, 2, 3}));
    EXPECT_EQ(n.configIndex(), 0u);
}

TEST(RaftNodeMembership, AJoiningServerIsNotAMemberAndStaysQuiet) {
    Config c = config(4, {});
    c.joining = true;
    c.bootstrapConfigEntry = true;  // makes no sense for a joiner and must be ignored
    RaftNode n(c, {}, {});
    EXPECT_FALSE(n.isMember());
    EXPECT_EQ(n.log().lastIndex(), 0u);

    for (int i = 0; i < 1000; ++i) n.tick();
    EXPECT_EQ(n.role(), Role::Follower);
    EXPECT_EQ(n.term(), 0u);  // it never started an election
    EXPECT_FALSE(n.hasReady());

    // But it DOES vote. It may already count towards a majority of the configuration it
    // hasn't heard about yet, and the leader who would tell it can only be elected with its help.
    n.step(msg(1, 4, 1, RequestVote{0, 0}));
    EXPECT_TRUE(std::get<RequestVoteResponse>(n.takeReady().messages.at(0).body).granted);
}

// Regression test (found by the membership chaos test): a non-member's clock must keep running, or the
// last leader it ever heard of stays "recent" for ever, and it ignores every vote request from then on,
// while its vote may be needed for a majority of a configuration it hasn't learned about.
TEST(RaftNodeMembership, ANonMemberStopsHonouringALongGoneLeader) {
    Config c = config(4, {});
    c.joining = true;
    RaftNode n(c, {}, {});
    n.step(msg(1, 4, 1, AppendEntries{0, 0, {}, 0}));  // it hears a leader once...
    n.takeReady();

    n.step(msg(2, 4, 2, RequestVote{0, 0}));
    EXPECT_EQ(n.term(), 1u) << "right after the leader's heartbeat, vote requests are ignored";

    for (int i = 0; i < 30; ++i) n.tick();  // ...and then the leader goes quiet
    n.step(msg(2, 4, 2, RequestVote{0, 0}));
    EXPECT_EQ(n.term(), 2u);
    EXPECT_TRUE(std::get<RequestVoteResponse>(n.takeReady().messages.at(0).body).granted);
}

TEST(RaftNodeMembership, AJoiningServerLearnsItsMembershipFromTheLog) {
    Config c = config(4, {});
    c.joining = true;
    RaftNode n(c, {}, {});

    const Members four = ids({1, 2, 3, 4});
    n.step(msg(1, 4, 1, AppendEntries{0, 0, {configEntry(0, 1, ids({1, 2, 3})), configEntry(1, 2, four)}, 0}));
    EXPECT_TRUE(n.isMember());
    EXPECT_EQ(n.members(), four);
    EXPECT_EQ(n.configIndex(), 2u);  // the newest configuration in the log counts, committed or not

    // Now it takes part in elections like anybody else.
    for (int i = 0; i < 1000 && n.role() == Role::Follower; ++i) n.tick();
    EXPECT_EQ(n.role(), Role::Candidate);
}

TEST(RaftNodeMembership, ConfigChangesWaitForTheLeaderToBeReady) {
    RaftNode n = leaderOfThree();
    ConfigError err;

    // Nothing from this leader's term has committed yet: it can't tell if an older change is still pending.
    EXPECT_FALSE(n.proposeConfigChange(ids({1, 2, 3, 4}), &err));
    EXPECT_EQ(err, ConfigError::NotReady);

    ack(n, 2, 1);
    EXPECT_TRUE(n.proposeConfigChange(ids({1, 2, 3, 4}), &err));
    EXPECT_EQ(err, ConfigError::None);

    RaftNode follower(config(1), {}, {});
    EXPECT_FALSE(follower.proposeConfigChange(ids({1, 2, 3, 4}), &err));
    EXPECT_EQ(err, ConfigError::NotLeader);
}

TEST(RaftNodeMembership, OnlyOneServerMayChangeAtATime) {
    RaftNode n = readyLeader();
    ConfigError err;

    EXPECT_FALSE(n.proposeConfigChange(ids({1, 2, 3, 4, 5}), &err)) << "two additions at once";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_FALSE(n.proposeConfigChange(ids({1}), &err)) << "two removals at once";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_FALSE(n.proposeConfigChange(ids({1, 2, 4}), &err)) << "one out, one in";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_FALSE(n.proposeConfigChange(ids({1, 2, 3}), &err)) << "no change at all";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_FALSE(n.proposeConfigChange({}, &err)) << "an empty cluster";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_FALSE(n.proposeConfigChange({{1, ""}, {2, ""}, {3, "moved:9"}}, &err)) << "a changed address is a removal plus an addition";
    EXPECT_EQ(err, ConfigError::Invalid);
    EXPECT_EQ(n.members(), ids({1, 2, 3}));  // none of them did anything
}

TEST(RaftNodeMembership, AddingAServerTakesEffectAtOnceAndCommitsWithTheNewQuorum) {
    RaftNode n = readyLeader();
    ConfigError err;

    auto index = n.proposeConfigChange(ids({1, 2, 3, 4}), &err);
    ASSERT_TRUE(index);
    EXPECT_EQ(*index, 2u);
    EXPECT_EQ(n.members(), ids({1, 2, 3, 4}));  // already, before the entry commits
    EXPECT_EQ(n.configIndex(), 2u);

    Ready rd = n.takeReady();
    EXPECT_TRUE(hasMessageTo<AppendEntries>(rd, 4)) << "the new server is replicated to from now on";
    ASSERT_EQ(rd.entries.size(), 1u);
    EXPECT_EQ(rd.entries[0].type, Entry::Type::Config);

    // The next change has to wait for this one.
    EXPECT_FALSE(n.proposeConfigChange(ids({1, 2, 3, 4, 5}), &err));
    EXPECT_EQ(err, ConfigError::Pending);

    // 4 members: a majority is 3. The leader and node 2 are only two.
    ack(n, 2, 2);
    EXPECT_EQ(n.commitIndex(), 1u);
    ack(n, 3, 2);
    EXPECT_EQ(n.commitIndex(), 2u);

    EXPECT_TRUE(n.proposeConfigChange(ids({1, 2, 3, 4, 5}), &err));  // now it may go on
}

TEST(RaftNodeMembership, RemovingAServerShrinksTheQuorumAndStopsReplicatingToIt) {
    RaftNode n = readyLeader();
    ASSERT_TRUE(n.proposeConfigChange(ids({1, 2}), nullptr));
    EXPECT_EQ(n.members(), ids({1, 2}));
    n.takeReady();

    ack(n, 2, 2);  // 2 members: the leader and node 2 are a majority; node 3 isn't needed
    EXPECT_EQ(n.commitIndex(), 2u);

    for (int i = 0; i < 3; ++i) n.tick();
    Ready heartbeat = n.takeReady();
    EXPECT_TRUE(hasMessageTo<AppendEntries>(heartbeat, 2));
    EXPECT_FALSE(hasMessageTo<AppendEntries>(heartbeat, 3)) << "a removed server gets nothing more";
}

TEST(RaftNodeMembership, ALeaderThatRemovesItselfStepsDownOnceTheChangeCommits) {
    RaftNode n = readyLeader();
    ASSERT_TRUE(n.proposeConfigChange(ids({2, 3}), nullptr));  // remove ourselves
    EXPECT_FALSE(n.isMember());
    EXPECT_EQ(n.role(), Role::Leader) << "it keeps leading until the change has committed";

    // Meanwhile it takes no new work.
    EXPECT_FALSE(n.propose("too late"));
    EXPECT_FALSE(n.requestRead());

    // The change must be stored on a majority of the NEW configuration {2, 3}: both of them.
    ack(n, 2, 2);
    EXPECT_EQ(n.commitIndex(), 1u);
    EXPECT_EQ(n.role(), Role::Leader);
    ack(n, 3, 2);
    EXPECT_EQ(n.commitIndex(), 2u);
    EXPECT_EQ(n.role(), Role::Follower);

    // And as a non-member it never starts an election again.
    for (int i = 0; i < 1000; ++i) n.tick();
    EXPECT_EQ(n.role(), Role::Follower);
}

// A configuration entry that gets overwritten (it never committed) must be undone.
TEST(RaftNodeMembership, AnOverwrittenConfigurationEntryIsUndone) {
    RaftNode n(config(2), {1, std::nullopt}, {});
    const Members four = ids({1, 2, 3, 4});

    n.step(msg(1, 2, 1, AppendEntries{0, 0, {configEntry(1, 1, four)}, 0}));
    ASSERT_EQ(n.members(), four);

    // A new leader of term 2 never saw that entry; its entry 1 is a different one.
    n.step(msg(3, 2, 2, AppendEntries{0, 0, {entry(2, 1, "something else")}, 0}));
    EXPECT_EQ(n.members(), ids({1, 2, 3}));
    EXPECT_EQ(n.configIndex(), 0u);
}

// Granting a vote is a matter of the log and one vote per term, not of the configuration;
// what the configuration decides is whose votes a candidate COUNTS.
TEST(RaftNodeMembership, OnlyMembersVotesAreCounted) {
    RaftNode n(config(1, {1, 2}), {}, {});  // a 2-server cluster
    n.step(msg(3, 1, 1, RequestVote{0, 0}));  // server 3 isn't in it, but may still be voted for
    EXPECT_TRUE(std::get<RequestVoteResponse>(n.takeReady().messages.at(0).body).granted);

    // A candidate ignores votes that arrive from a non-member.
    RaftNode c(config(1, {1, 2, 3}), {}, {});
    tickUntilCandidate(c);
    c.takeReady();
    c.step(msg(9, 1, c.term(), RequestVoteResponse{true}));
    EXPECT_EQ(c.role(), Role::Candidate);
}

TEST(RaftNodeMembership, TheConfigurationInASnapshotAndTheLogAfterIt) {
    Snapshot snap{5, 1, ids({1, 2, 3}), "state"};
    // A configuration entry after the snapshot (index 6) wins over the snapshot's.
    RaftNode n(config(1), {1, std::nullopt}, {configEntry(1, 6, ids({1, 2, 3, 4}))}, snap);
    EXPECT_EQ(n.members(), ids({1, 2, 3, 4}));
    EXPECT_EQ(n.configIndex(), 6u);

    // And a snapshot taken later records the configuration as of ITS index, not the newest.
    RaftNode leader(config(1, {1}), {1, std::nullopt}, {});
    tickUntilCandidate(leader);
    leader.takeReady();
    ASSERT_TRUE(leader.proposeConfigChange(ids({1, 2}), nullptr));  // at index 2
    leader.takeReady();
    ASSERT_TRUE(leader.compact(1, "state at 1"));
    EXPECT_EQ(leader.takeReady().snapshot->members, ids({1}));
}
