#pragma once

#include <optional>
#include <vector>

#include "raft/types.h"

namespace kv::raft {

// The replicated log, held in memory.
//
// Position 0 holds a sentinel entry (index 0, term 0), so "the entry before
// the first one" always exists. That removes special cases: an
// AppendEntries with prevLogIndex 0 matches every log.
class RaftLog {
public:
    RaftLog();

    // Restores a log from persisted entries (indices 1, 2, 3, ...).
    explicit RaftLog(std::vector<Entry> entries);

    Index lastIndex() const { return entries_.size() - 1; }
    Term lastTerm() const { return entries_.back().term; }

    // Term of the entry at `index`, or nullopt if the log is shorter.
    std::optional<Term> termAt(Index index) const;

    // True if we have an entry at `index` with term `term`.
    bool matches(Index index, Term term) const { return termAt(index) == term; }

    const Entry& at(Index index) const { return entries_.at(index); }

    // Copies of entries with index in [from, to). Clamped to the log's end.
    std::vector<Entry> slice(Index from, Index to) const;

    // Appends one entry; its index must be lastIndex() + 1.
    void append(Entry entry);

    // Removes the entry at `index` and everything after it.
    void truncateFrom(Index index);

    // Election rule: is a log ending with (lastIndex, lastTerm) at least as
    // up-to-date as this one? A higher last term wins; with equal terms, the
    // longer log wins.
    bool isUpToDate(Index lastIndex, Term lastTerm) const;

private:
    std::vector<Entry> entries_;  // entries_[i].index == i
};

}  // namespace kv::raft
