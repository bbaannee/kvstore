#include "storage/kv_store.h"

#include "storage/file_util.h"

namespace kv::storage {

KvStore::KvStore(const std::string& logPath) : recovery_(replay(logPath)), log_(logPath) {}

void KvStore::put(const std::string& key, const std::string& value) {
    LogRecord rec = LogRecord::put(key, value);
    log_.append(rec);  // log first...
    apply(rec);        // ...then the map
}

bool KvStore::del(const std::string& key) {
    if (!data_.contains(key)) return false;
    LogRecord rec = LogRecord::del(key);
    log_.append(rec);
    apply(rec);
    return true;
}

std::optional<std::string> KvStore::get(const std::string& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) return std::nullopt;
    return it->second;
}

RecoveryInfo KvStore::replay(const std::string& logPath) {
    RecoveryInfo info;
    LogReader reader(logPath);
    LogRecord rec;
    ReadStatus status;
    while ((status = reader.next(rec)) == ReadStatus::Ok) {
        apply(rec);
        ++info.recordsReplayed;
    }
    info.validBytes = reader.validBytes();

    // Damage a crash can't explain: stop rather than lose or invent data.
    if (status == ReadStatus::Corrupt)
        throw CorruptLogError(logPath + " is corrupted: " + reader.error());

    // If we crashed mid-write, cut the half-written record off. Otherwise new
    // records would be appended after the garbage, and the next replay would
    // stop at the garbage and silently lose them.
    if (status == ReadStatus::Torn) {
        truncateFile(logPath, info.validBytes);
        info.truncatedTornTail = true;
    }
    return info;
}

void KvStore::apply(const LogRecord& rec) {
    if (rec.type == LogRecord::Type::Put) data_[rec.key] = rec.value;
    else if (rec.type == LogRecord::Type::Del) data_.erase(rec.key);
    // Get changes nothing.
}

}  // namespace kv::storage
