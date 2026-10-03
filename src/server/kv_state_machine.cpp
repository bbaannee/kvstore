#include "server/kv_state_machine.h"

#include <algorithm>
#include <vector>

#include "common/coding.h"
#include "storage/log_record.h"

namespace kv::server {

using storage::LogRecord;

namespace {

constexpr std::uint8_t kStateTag = 0x4B;  // 'K'
constexpr std::size_t kMaxItemSize = 64 * 1024 * 1024;

// An entry that holds at least this many bytes: used to bound counts before allocating.
constexpr std::size_t kMinMapItem = 4 + 4;
constexpr std::size_t kMinSessionItem = 8 + 8 + 8 + 1 + 4;

// Restoring writes the data in batches of about this size.
constexpr std::size_t kRestoreBatchBytes = 4 * 1024 * 1024;

const char* const kUseCounterKey = "usecounter";
constexpr char kSessionPrefix = 's';

void putString(std::string& out, const std::string& s) {
    putFixed32(out, static_cast<std::uint32_t>(s.size()));
    out += s;
}

// A session's key in the storage's bookkeeping: "s" + the client id, big-endian.
std::string sessionKey(std::uint64_t clientId) {
    std::string key(1, kSessionPrefix);
    for (int shift = 56; shift >= 0; shift -= 8) key += static_cast<char>((clientId >> shift) & 0xFF);
    return key;
}

std::uint64_t clientIdOf(const std::string& key) {
    std::uint64_t id = 0;
    for (std::size_t i = 1; i < key.size(); ++i) id = (id << 8) | static_cast<unsigned char>(key[i]);
    return id;
}

// [seq: u64][last_used: u64][found: u8][value: u32 len + bytes]
std::string encodeSession(std::uint64_t seq, std::uint64_t lastUsed, bool found, const std::string& value) {
    std::string out;
    putFixed64(out, seq);
    putFixed64(out, lastUsed);
    out += static_cast<char>(found ? 1 : 0);
    putString(out, value);
    return out;
}

}  // namespace

KvStateMachine::KvStateMachine(std::size_t maxSessions, std::unique_ptr<KvStorage> storage)
    : maxSessions_(maxSessions), storage_(std::move(storage)) {
    loadSessions();
}

// A storage that was already used (a restart with a persistent engine) brings its sessions back.
void KvStateMachine::loadSessions() {
    if (auto counter = storage_->getMeta(kUseCounterKey); counter && counter->size() == 8)
        useCounter_ = decodeFixed64(counter->data());

    storage_->forEachMeta(std::string(1, kSessionPrefix), [&](const std::string& key, const std::string& bytes) {
        ByteReader in(bytes);
        Session s;
        std::uint8_t found;
        if (!in.getFixed64(s.seq) || !in.getFixed64(s.lastUsed) || !in.getU8(found) ||
            !in.getLengthPrefixed(kMaxItemSize, s.result.value) || !in.atEnd())
            return;  // (written by this class, so malformed means damaged: skipping it only forgets one client)
        s.result.found = found == 1;
        byUse_[s.lastUsed] = clientIdOf(key);
        sessions_.emplace(clientIdOf(key), std::move(s));
    });
}

void KvStateMachine::beginBatch() {
    if (batchDepth_++ == 0) storage_->beginBatch();
}

void KvStateMachine::endBatch() {
    if (--batchDepth_ == 0) storage_->commitBatch();
}

ApplyResult KvStateMachine::apply(const raft::Entry& entry) {
    beginBatch();
    ApplyResult result;
    if (entry.type == raft::Entry::Type::Command) result = applyCommand(entry);
    storage_->setAppliedIndex(entry.index);  // in the same batch as the change it describes
    endBatch();
    return result;
}

ApplyResult KvStateMachine::applyCommand(const raft::Entry& entry) {
    Command cmd;
    if (!decodeCommand(entry.data, cmd)) return {};

    if (cmd.clientId == 0) return execute(cmd.op);  // anonymous: no deduplication

    auto it = sessions_.find(cmd.clientId);
    if (it != sessions_.end() && cmd.seq <= it->second.seq) {
        // A retry (same seq) gets the original answer; an older seq is a
        // stale duplicate whose client has long moved on.
        ApplyResult cached = cmd.seq == it->second.seq ? it->second.result : ApplyResult{};
        touch(cmd.clientId, it->second);
        saveUseCounter();
        return cached;
    }

    ApplyResult result = execute(cmd.op);
    if (it == sessions_.end()) {
        // A new client. Make room first, forgetting whoever was used longest ago.
        while (sessions_.size() >= maxSessions_ && !byUse_.empty()) {
            const std::uint64_t oldest = byUse_.begin()->second;
            sessions_.erase(oldest);
            storage_->eraseMeta(sessionKey(oldest));
            byUse_.erase(byUse_.begin());
        }
        it = sessions_.emplace(cmd.clientId, Session{}).first;
    }
    it->second.seq = cmd.seq;
    it->second.result = result;
    touch(cmd.clientId, it->second);
    saveUseCounter();
    return result;
}

// Records that this client was just used: it moves to the "newest" end of the eviction order.
void KvStateMachine::touch(std::uint64_t clientId, Session& session) {
    if (session.lastUsed != 0) byUse_.erase(session.lastUsed);
    session.lastUsed = ++useCounter_;
    byUse_[session.lastUsed] = clientId;
    storage_->putMeta(sessionKey(clientId), encodeSession(session.seq, session.lastUsed, session.result.found, session.result.value));
}

void KvStateMachine::saveUseCounter() {
    std::string bytes;
    putFixed64(bytes, useCounter_);
    storage_->putMeta(kUseCounterKey, bytes);
}

ApplyResult KvStateMachine::execute(const LogRecord& op) {
    switch (op.type) {
        case LogRecord::Type::Put:
            storage_->put(op.key, op.value);
            return {true, {}};
        case LogRecord::Type::Del:
            return {storage_->erase(op.key), {}};
        case LogRecord::Type::Get:
            return read(op.key);
    }
    return {};
}

ApplyResult KvStateMachine::read(const std::string& key) const {
    auto value = storage_->get(key);
    if (!value) return {};
    return {true, std::move(*value)};
}

std::unordered_map<std::string, std::string> KvStateMachine::data() const {
    std::unordered_map<std::string, std::string> out;
    storage_->forEach([&](const std::string& k, const std::string& v) { out.emplace(k, v); });
    return out;
}

std::string KvStateMachine::serialize() const {
    std::string out;
    out += static_cast<char>(kStateTag);
    putFixed64(out, useCounter_);

    std::vector<std::pair<std::string, std::string>> items;
    storage_->forEach([&](const std::string& k, const std::string& v) { items.emplace_back(k, v); });
    std::sort(items.begin(), items.end());  // the same bytes whatever order the storage iterates in
    putFixed64(out, items.size());
    for (const auto& [k, v] : items) {
        putString(out, k);
        putString(out, v);
    }

    std::vector<std::uint64_t> clients;
    clients.reserve(sessions_.size());
    for (const auto& s : sessions_) clients.push_back(s.first);
    std::sort(clients.begin(), clients.end());
    putFixed64(out, clients.size());
    for (std::uint64_t id : clients) {
        const Session& s = sessions_.at(id);
        putFixed64(out, id);
        putFixed64(out, s.seq);
        putFixed64(out, s.lastUsed);
        out += static_cast<char>(s.result.found ? 1 : 0);
        putString(out, s.result.value);
    }
    return out;
}

bool KvStateMachine::restore(std::string_view bytes, std::uint64_t appliedIndex) {
    // Parse everything first, so a malformed snapshot changes nothing but leaves the state empty.
    std::vector<std::pair<std::string, std::string>> items;
    std::unordered_map<std::uint64_t, Session> sessions;
    std::map<std::uint64_t, std::uint64_t> byUse;
    std::uint64_t counter = 0;

    auto parse = [&]() -> bool {
        ByteReader in(bytes);
        std::uint8_t tag;
        std::uint64_t mapSize, sessionCount;
        if (!in.getU8(tag) || tag != kStateTag || !in.getFixed64(counter) || !in.getFixed64(mapSize) ||
            mapSize > in.remaining() / kMinMapItem)
            return false;
        items.reserve(static_cast<std::size_t>(mapSize));
        for (std::uint64_t i = 0; i < mapSize; ++i) {
            std::string key, value;
            if (!in.getLengthPrefixed(kMaxItemSize, key) || !in.getLengthPrefixed(kMaxItemSize, value)) return false;
            items.emplace_back(std::move(key), std::move(value));
        }
        if (!in.getFixed64(sessionCount) || sessionCount > in.remaining() / kMinSessionItem) return false;
        for (std::uint64_t i = 0; i < sessionCount; ++i) {
            std::uint64_t id;
            std::uint8_t found;
            Session s;
            if (!in.getFixed64(id) || !in.getFixed64(s.seq) || !in.getFixed64(s.lastUsed) || !in.getU8(found) ||
                found > 1 || !in.getLengthPrefixed(kMaxItemSize, s.result.value))
                return false;
            s.result.found = found == 1;
            if (s.lastUsed == 0 || s.lastUsed > counter || byUse.count(s.lastUsed)) return false;
            byUse[s.lastUsed] = id;
            sessions.emplace(id, std::move(s));
        }
        return in.atEnd() && sessions.size() == sessionCount;
    };

    storage_->clear();  // (the applied index is 0 from here until the final batch below)
    sessions_.clear();
    byUse_.clear();
    useCounter_ = 0;
    batchDepth_ = 0;
    if (!parse()) return false;

    // The data goes in in chunks, then the bookkeeping, with the applied index last. A crash in
    // between leaves the applied index at 0, so the next start restores again from the snapshot.
    std::size_t chunkBytes = 0;
    storage_->beginBatch();
    for (const auto& [k, v] : items) {
        storage_->put(k, v);
        chunkBytes += k.size() + v.size();
        if (chunkBytes >= kRestoreBatchBytes) {
            storage_->commitBatch();
            storage_->beginBatch();
            chunkBytes = 0;
        }
    }
    sessions_ = std::move(sessions);
    byUse_ = std::move(byUse);
    useCounter_ = counter;
    for (const auto& [id, s] : sessions_)
        storage_->putMeta(sessionKey(id), encodeSession(s.seq, s.lastUsed, s.result.found, s.result.value));
    saveUseCounter();
    storage_->setAppliedIndex(appliedIndex);
    storage_->commitBatch();
    return true;
}

}  // namespace kv::server
