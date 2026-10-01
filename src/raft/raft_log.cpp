#include "raft/raft_log.h"

#include <algorithm>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace kv::raft {

RaftLog::RaftLog() : entries_{Entry{}} {}

RaftLog::RaftLog(std::vector<Entry> entries) : RaftLog() {
    for (auto& e : entries) append(std::move(e));
}

std::optional<Term> RaftLog::termAt(Index index) const {
    if (index > lastIndex()) return std::nullopt;
    return entries_[index].term;
}

std::vector<Entry> RaftLog::slice(Index from, Index to) const {
    to = std::min(to, lastIndex() + 1);
    if (from >= to) return {};
    return {entries_.begin() + static_cast<std::ptrdiff_t>(from),
            entries_.begin() + static_cast<std::ptrdiff_t>(to)};
}

void RaftLog::append(Entry entry) {
    if (entry.index != lastIndex() + 1)
        throw std::logic_error("raft log: appending index " + std::to_string(entry.index) +
                               " after " + std::to_string(lastIndex()));
    if (entry.term < lastTerm())
        throw std::logic_error("raft log: terms must never decrease along the log");
    entries_.push_back(std::move(entry));
}

void RaftLog::truncateFrom(Index index) {
    if (index == 0) throw std::logic_error("raft log: cannot remove the sentinel");
    if (index <= lastIndex()) entries_.resize(index);
}

bool RaftLog::isUpToDate(Index candidateLastIndex, Term candidateLastTerm) const {
    if (candidateLastTerm != lastTerm()) return candidateLastTerm > lastTerm();
    return candidateLastIndex >= lastIndex();
}

}  // namespace kv::raft
