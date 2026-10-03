#include "raft/raft_log.h"

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace kv::raft {

RaftLog::RaftLog(std::vector<Entry> entries) {
    for (auto& e : entries) append(std::move(e));
}

RaftLog::RaftLog(Index baseIndex, Term baseTerm, std::vector<Entry> entries)
    : baseIndex_(baseIndex), baseTerm_(baseTerm) {
    for (auto& e : entries) append(std::move(e));
}

std::optional<Term> RaftLog::termAt(Index index) const {
    if (index == baseIndex_) return baseTerm_;
    if (index < baseIndex_ || index > lastIndex()) return std::nullopt;
    return entries_[index - baseIndex_ - 1].term;
}

std::vector<Entry> RaftLog::slice(Index from, Index to) const {
    from = std::max(from, firstIndex());
    to = std::min(to, lastIndex() + 1);
    if (from >= to) return {};
    return {entries_.begin() + static_cast<std::ptrdiff_t>(from - baseIndex_ - 1),
            entries_.begin() + static_cast<std::ptrdiff_t>(to - baseIndex_ - 1)};
}

void RaftLog::append(Entry entry) {
    if (entry.index != lastIndex() + 1)
        throw std::logic_error("raft log: appending index " + std::to_string(entry.index) + " after " +
                               std::to_string(lastIndex()));
    if (entry.term < lastTerm()) throw std::logic_error("raft log: terms must never decrease along the log");
    entries_.push_back(std::move(entry));
}

void RaftLog::truncateFrom(Index index) {
    if (index <= baseIndex_) throw std::logic_error("raft log: cannot remove compacted entries");
    if (index <= lastIndex()) entries_.resize(index - baseIndex_ - 1);
}

void RaftLog::compactTo(Index index, Term term) {
    if (index < baseIndex_) throw std::logic_error("raft log: compacting backwards");
    if (index >= lastIndex()) {
        entries_.clear();
    } else {
        entries_.erase(entries_.begin(), entries_.begin() + static_cast<std::ptrdiff_t>(index - baseIndex_));
    }
    baseIndex_ = index;
    baseTerm_ = term;
}

void RaftLog::resetTo(Index index, Term term) {
    entries_.clear();
    baseIndex_ = index;
    baseTerm_ = term;
}

bool RaftLog::isUpToDate(Index candidateLastIndex, Term candidateLastTerm) const {
    if (candidateLastTerm != lastTerm()) return candidateLastTerm > lastTerm();
    return candidateLastIndex >= lastIndex();
}

}  // namespace kv::raft
