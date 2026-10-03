#include "raft/raft_storage.h"

#include <cstdint>
#include <filesystem>
#include <stdexcept>

#include "common/coding.h"
#include "raft/entry_codec.h"
#include "raft/members_codec.h"
#include "storage/file_util.h"
#include "storage/log_format.h"
#include "storage/log_reader.h"
#include "storage/log_record.h"

namespace kv::raft {

namespace {

constexpr std::uint8_t kUpdateTag = 0x52;    // 'R'
constexpr std::uint8_t kSnapshotTag = 0x53;  // 'S'
constexpr std::uint8_t kHasHardState = 0x01;

}  // namespace

std::string encodeUpdate(const StorageUpdate& update) {
    std::string out;
    out += static_cast<char>(kUpdateTag);
    out += static_cast<char>(update.hardState ? kHasHardState : 0);
    if (update.hardState) {
        putFixed64(out, update.hardState->term);
        out += static_cast<char>(update.hardState->votedFor ? 1 : 0);
        putFixed64(out, update.hardState->votedFor.value_or(0));
    }
    putFixed32(out, static_cast<std::uint32_t>(update.entries.size()));
    for (const Entry& e : update.entries) putEntry(out, e);
    return out;
}

bool decodeUpdate(std::string_view payload, StorageUpdate& out) {
    out = StorageUpdate{};
    ByteReader r(payload);

    std::uint8_t tag, flags;
    if (!r.getU8(tag) || tag != kUpdateTag || !r.getU8(flags) || (flags & ~kHasHardState)) return false;

    if (flags & kHasHardState) {
        HardState hs;
        std::uint8_t hasVote;
        std::uint64_t votedFor;
        if (!r.getFixed64(hs.term) || !r.getU8(hasVote) || hasVote > 1 || !r.getFixed64(votedFor)) return false;
        if (hasVote) hs.votedFor = votedFor;
        out.hardState = hs;
    }

    std::uint32_t count;
    if (!r.getFixed32(count) || count > r.remaining() / kMinEncodedEntrySize) return false;  // no huge reserve()
    out.entries.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        Entry e;
        if (!getEntry(r, e)) return false;
        out.entries.push_back(std::move(e));
    }
    return r.atEnd();
}

std::string encodeSnapshot(const Snapshot& snapshot) {
    std::string out;
    out += static_cast<char>(kSnapshotTag);
    putFixed64(out, snapshot.index);
    putFixed64(out, snapshot.term);
    putMembers(out, snapshot.members);
    putFixed32(out, static_cast<std::uint32_t>(snapshot.data.size()));
    out += snapshot.data;
    return out;
}

bool decodeSnapshot(std::string_view payload, Snapshot& out) {
    out = Snapshot{};
    ByteReader r(payload);
    std::uint8_t tag;
    if (!r.getU8(tag) || tag != kSnapshotTag || !r.getFixed64(out.index) || !r.getFixed64(out.term)) return false;
    return getMembers(r, out.members) && r.getLengthPrefixed(storage::kMaxPayloadSize, out.data) && r.atEnd();
}

namespace {

// Applies one recovered Update record to `state`. Returns an empty string on
// success, or a description of the rule it breaks. A record that breaks Raft's
// rules passed its checksum, so a buggy writer (or a bug in us) produced it;
// replaying it would silently build a wrong state.
std::string applyUpdate(RaftState& state, const StorageUpdate& u) {
    if (u.hardState) {
        const HardState& hs = *u.hardState;
        if (hs.term < state.hardState.term) return "term went backwards";
        if (hs.term == state.hardState.term && state.hardState.votedFor && hs.votedFor != state.hardState.votedFor)
            return "vote changed within a term";
        state.hardState = hs;
    }

    if (!u.entries.empty()) {
        const Index base = state.snapshot ? state.snapshot->index : 0;
        const Term baseTerm = state.snapshot ? state.snapshot->term : 0;
        const Index first = u.entries.front().index;
        if (first <= base) return "entries at or below the snapshot";
        if (first > base + state.log.size() + 1) return "gap in the log";

        Term prevTerm = first > base + 1 ? state.log[first - base - 2].term : baseTerm;
        for (std::size_t i = 0; i < u.entries.size(); ++i) {
            if (u.entries[i].index != first + i) return "log entries are not contiguous";
            if (u.entries[i].term < prevTerm) return "entry terms go backwards";
            prevTerm = u.entries[i].term;
        }

        // Overwrite from `first` on: this is how conflicting entries get replaced.
        state.log.erase(state.log.begin() + static_cast<std::ptrdiff_t>(first - base - 1), state.log.end());
        state.log.insert(state.log.end(), u.entries.begin(), u.entries.end());
    }
    return "";
}

// Applies a Snapshot record: the same decision a node makes when it installs a snapshot.
std::string applySnapshot(RaftState& state, Snapshot snapshot) {
    const Index base = state.snapshot ? state.snapshot->index : 0;
    if (snapshot.index < base) return "a snapshot older than the current one";

    // Entries at or below the snapshot are covered. If our log agrees with the
    // snapshot at its last index, the entries after it are still valid; if not,
    // they belong to a history the cluster has moved on from.
    const Index last = base + state.log.size();
    bool agrees = false;
    if (snapshot.index == base) agrees = state.snapshot && state.snapshot->term == snapshot.term;
    else if (snapshot.index <= last) agrees = state.log[snapshot.index - base - 1].term == snapshot.term;

    if (agrees && snapshot.index > base) {
        state.log.erase(state.log.begin(), state.log.begin() + static_cast<std::ptrdiff_t>(snapshot.index - base));
    } else if (!agrees) {
        state.log.clear();
    }
    state.snapshot = std::move(snapshot);
    return "";
}

}  // namespace

RaftStorage::RaftStorage(const std::string& path, bool syncOnAppend)
    : path_(path), sync_(syncOnAppend), recovery_(recover(path)), hardState_(recovery_.state.hardState) {
    writer_ = std::make_unique<storage::LogWriter>(path_, sync_);
}

void RaftStorage::save(const StorageUpdate& update) {
    if (update.empty()) return;
    if (update.hardState) hardState_ = *update.hardState;

    if (update.snapshot) {
        compact(*update.snapshot, update.entries);
        return;
    }
    writer_->appendPayload(encodeUpdate(StorageUpdate{update.hardState, update.entries, std::nullopt}));
}

// Replaces the whole file with [Snapshot][Update]. The rename is the commit
// point: before it, the old file is intact; after it, the new one is.
void RaftStorage::compact(const Snapshot& snapshot, const std::vector<Entry>& entries) {
    const std::string snapshotPayload = encodeSnapshot(snapshot);
    if (snapshotPayload.size() > storage::kMaxPayloadSize) throw std::runtime_error("raft: snapshot is too large to store");

    const std::string temp = path_ + ".tmp";
    std::filesystem::remove(temp);
    {
        storage::LogWriter w(temp, sync_);  // creates the file; each append is fsynced
        w.appendPayload(snapshotPayload);
        w.appendPayload(encodeUpdate(StorageUpdate{hardState_, entries, std::nullopt}));
    }
    writer_.reset();  // close the old file before replacing it
    std::filesystem::rename(temp, path_);
    if (sync_) storage::fsyncParentDir(path_);
    writer_ = std::make_unique<storage::LogWriter>(path_, sync_);
}

RaftStorage::Recovery RaftStorage::recover(const std::string& path) {
    Recovery rec;

    // A compaction that crashed before its rename: the old file is still the truth.
    std::filesystem::remove(path + ".tmp");

    storage::LogReader reader(path);
    std::string payload;
    storage::ReadStatus status;

    while (true) {
        const std::uint64_t start = reader.validBytes();
        status = reader.nextPayload(payload);
        if (status != storage::ReadStatus::Ok) break;

        std::string problem;
        if (!payload.empty() && static_cast<std::uint8_t>(payload[0]) == kSnapshotTag) {
            Snapshot snapshot;
            problem = decodeSnapshot(payload, snapshot) ? applySnapshot(rec.state, std::move(snapshot))
                                                        : "malformed snapshot";
        } else {
            StorageUpdate update;
            problem = decodeUpdate(payload, update) ? applyUpdate(rec.state, update) : "malformed update";
        }
        if (!problem.empty())
            throw storage::CorruptLogError(path + " is corrupted: " + problem + " in record at byte " +
                                           std::to_string(start));
        ++rec.updatesReplayed;
    }

    if (status == storage::ReadStatus::Corrupt)
        throw storage::CorruptLogError(path + " is corrupted: " + reader.error());

    // A crash mid-write left a partial record at the end: cut it off, so new
    // records aren't appended behind garbage (see KvStore::replay).
    if (status == storage::ReadStatus::Torn) {
        storage::truncateFile(path, reader.validBytes());
        rec.truncatedTornTail = true;
    }
    return rec;
}

}  // namespace kv::raft
