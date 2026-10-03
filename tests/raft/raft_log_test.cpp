#include "raft/raft_log.h"

#include <gtest/gtest.h>

using namespace kv::raft;

namespace {

Entry entry(Term term, Index index) { return Entry{term, index, Entry::Type::Command, "e" + std::to_string(index)}; }

// A log of entries 1..n, all in term `term`.
RaftLog logOf(Index n, Term term = 1) {
    std::vector<Entry> entries;
    for (Index i = 1; i <= n; ++i) entries.push_back(entry(term, i));
    return RaftLog(std::move(entries));
}

}  // namespace

TEST(RaftLog, EmptyLogHasABaseAtZero) {
    RaftLog log;
    EXPECT_EQ(log.baseIndex(), 0u);
    EXPECT_EQ(log.lastIndex(), 0u);
    EXPECT_EQ(log.lastTerm(), 0u);
    EXPECT_EQ(log.termAt(0), 0u);  // "the entry before the first" always exists
    EXPECT_FALSE(log.termAt(1));
    EXPECT_TRUE(log.matches(0, 0));
    EXPECT_TRUE(log.slice(0, 10).empty());
}

TEST(RaftLog, AppendTermAtSliceAndTruncate) {
    RaftLog log = logOf(5);
    EXPECT_EQ(log.lastIndex(), 5u);
    EXPECT_EQ(log.termAt(3), 1u);
    EXPECT_FALSE(log.termAt(6));
    EXPECT_TRUE(log.matches(5, 1));
    EXPECT_FALSE(log.matches(5, 2));

    auto part = log.slice(2, 5);
    ASSERT_EQ(part.size(), 3u);
    EXPECT_EQ(part.front().index, 2u);
    EXPECT_EQ(part.back().index, 4u);
    EXPECT_EQ(log.slice(4, 100).size(), 2u);  // clamped to the end

    log.truncateFrom(4);
    EXPECT_EQ(log.lastIndex(), 3u);
    log.truncateFrom(10);  // beyond the end: nothing to do
    EXPECT_EQ(log.lastIndex(), 3u);
}

TEST(RaftLog, AppendEnforcesInvariants) {
    RaftLog log = logOf(2, 5);
    EXPECT_THROW(log.append(entry(5, 4)), std::logic_error);  // a gap
    EXPECT_THROW(log.append(entry(5, 2)), std::logic_error);  // not the next index
    EXPECT_THROW(log.append(entry(4, 3)), std::logic_error);  // terms never decrease
    log.append(entry(5, 3));
    log.append(entry(6, 4));
    EXPECT_EQ(log.lastTerm(), 6u);
}

TEST(RaftLog, CompactionKeepsTheSuffixAndMovesTheBase) {
    RaftLog log = logOf(5);
    log.compactTo(3, 1);

    EXPECT_EQ(log.baseIndex(), 3u);
    EXPECT_EQ(log.baseTerm(), 1u);
    EXPECT_EQ(log.firstIndex(), 4u);
    EXPECT_EQ(log.lastIndex(), 5u);

    EXPECT_EQ(log.termAt(3), 1u);        // the base still answers (the consistency check needs it)
    EXPECT_FALSE(log.termAt(2));          // compacted: unknown
    EXPECT_EQ(log.at(4).data, "e4");
    EXPECT_EQ(log.slice(1, 6).size(), 2u);  // clamps to what we still hold
    EXPECT_EQ(log.slice(1, 6).front().index, 4u);

    EXPECT_THROW(log.truncateFrom(3), std::logic_error);  // compacted entries are committed
    log.truncateFrom(5);
    EXPECT_EQ(log.lastIndex(), 4u);
    log.append(entry(1, 5));
    EXPECT_EQ(log.lastIndex(), 5u);
}

TEST(RaftLog, CompactingEverythingLeavesOnlyTheBase) {
    RaftLog log = logOf(4);
    log.compactTo(4, 1);
    EXPECT_EQ(log.lastIndex(), 4u);
    EXPECT_EQ(log.lastTerm(), 1u);
    EXPECT_TRUE(log.slice(0, 100).empty());
    log.append(entry(1, 5));  // appending continues right after the base
    EXPECT_EQ(log.at(5).data, "e5");

    EXPECT_THROW(log.compactTo(2, 1), std::logic_error);  // never backwards
}

TEST(RaftLog, ResetDiscardsEverything) {
    RaftLog log = logOf(3);
    log.resetTo(10, 4);
    EXPECT_EQ(log.baseIndex(), 10u);
    EXPECT_EQ(log.lastIndex(), 10u);
    EXPECT_EQ(log.lastTerm(), 4u);
    EXPECT_FALSE(log.termAt(3));
    log.append(Entry{4, 11, Entry::Type::NoOp, {}});
    EXPECT_EQ(log.lastIndex(), 11u);
}

TEST(RaftLog, RestoreFromABase) {
    RaftLog log(7, 2, {entry(2, 8), entry(3, 9)});
    EXPECT_EQ(log.baseIndex(), 7u);
    EXPECT_EQ(log.lastIndex(), 9u);
    EXPECT_EQ(log.lastTerm(), 3u);
    EXPECT_EQ(log.termAt(7), 2u);
    EXPECT_EQ(log.termAt(8), 2u);
    EXPECT_THROW(RaftLog(7, 2, {entry(2, 9)}), std::logic_error);  // doesn't continue after the base
}

TEST(RaftLog, UpToDateComparisonUsesTheBaseWhenEmpty) {
    RaftLog log(5, 3, {});  // everything compacted, last entry (5, term 3)
    EXPECT_TRUE(log.isUpToDate(5, 3));
    EXPECT_TRUE(log.isUpToDate(1, 4));   // a higher last term wins
    EXPECT_FALSE(log.isUpToDate(9, 2));  // a lower term loses, however long
    EXPECT_FALSE(log.isUpToDate(4, 3));  // same term, shorter
}
