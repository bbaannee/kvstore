#pragma once

#include <optional>
#include <vector>

#include "raft/types.h"

namespace kv::raft {

// The replicated log, held in memory.
//
// After a snapshot the oldest entries are gone. What remains is described by
// a "base": the index and term of the last entry the snapshot covers
// (0 and 0 for a log that was never compacted). The base plays the role of
// an entry just before the first one we hold, so "the entry before the
// first one" always has a known index and term. That removes special cases:
// an AppendEntries with prevLogIndex equal to the base matches.
class RaftLog {
public:
    RaftLog() = default;

    // Restores a never-compacted log from persisted entries (indices 1, 2, 3, ...).
    explicit RaftLog(std::vector<Entry> entries);

    // Restores a compacted log: `entries` continue right after the base.
    RaftLog(Index baseIndex, Term baseTerm, std::vector<Entry> entries);

    Index baseIndex() const { return baseIndex_; }
    Term baseTerm() const { return baseTerm_; }
    Index firstIndex() const { return baseIndex_ + 1; }  // the oldest entry we still hold (if any)
    Index lastIndex() const { return baseIndex_ + entries_.size(); }
    Term lastTerm() const { return entries_.empty() ? baseTerm_ : entries_.back().term; }

    // Term of the entry at `index`: known for the base and for held entries,
    // unknown (nullopt) for compacted entries and for entries past the end.
    std::optional<Term> termAt(Index index) const;

    // True if we have an entry (or the base) at `index` with term `term`.
    bool matches(Index index, Term term) const { return termAt(index) == term; }

    // A held entry. `index` must be in [firstIndex(), lastIndex()].
    const Entry& at(Index index) const { return entries_.at(index - baseIndex_ - 1); }

    // Copies of entries with index in [from, to). Clamped to what we hold.
    std::vector<Entry> slice(Index from, Index to) const;

    // Appends one entry; its index must be lastIndex() + 1.
    void append(Entry entry);

    // Removes the entry at `index` and everything after it. `index` must be
    // after the base: compacted entries are committed and can't be removed.
    void truncateFrom(Index index);

    // Drops every entry up to and including `index` and makes (index, term)
    // the new base. Entries after `index` are kept.
    void compactTo(Index index, Term term);

    // Throws away every entry and makes (index, term) the base. Used when a
    // snapshot from the leader covers more than we have, or conflicts with it.
    void resetTo(Index index, Term term);

    // Election rule: is a log ending with (lastIndex, lastTerm) at least as
    // up-to-date as this one? A higher last term wins; with equal terms, the
    // longer log wins.
    bool isUpToDate(Index lastIndex, Term lastTerm) const;

private:
    Index baseIndex_ = 0;
    Term baseTerm_ = 0;
    std::vector<Entry> entries_;  // entries_[i].index == baseIndex_ + 1 + i
};

}  // namespace kv::raft
