#include "storage/log_record.h"

#include <cstdint>

#include "common/coding.h"

namespace kv::storage {

namespace {

void putField(std::string& out, const std::string& data) {
    putFixed32(out, static_cast<std::uint32_t>(data.size()));
    out += data;
}

// Reads [len: u32][data] at `pos` and advances `pos`. Bounds-checked.
bool getField(std::string_view in, std::size_t& pos, std::string& out) {
    if (in.size() - pos < 4) return false;
    std::uint32_t len = decodeFixed32(in.data() + pos);
    pos += 4;
    if (len > kMaxFieldSize || in.size() - pos < len) return false;
    out.assign(in.substr(pos, len));
    pos += len;
    return true;
}

}  // namespace

std::string encodePayload(const LogRecord& rec) {
    std::string out;
    out += static_cast<char>(rec.type);
    putField(out, rec.key);
    if (rec.type == LogRecord::Type::Put) putField(out, rec.value);
    return out;
}

bool decodePayload(std::string_view payload, LogRecord& out) {
    if (payload.empty()) return false;
    out = LogRecord{};

    auto type = static_cast<LogRecord::Type>(payload[0]);
    if (type != LogRecord::Type::Put && type != LogRecord::Type::Del) return false;
    out.type = type;

    std::size_t pos = 1;
    if (!getField(payload, pos, out.key)) return false;
    if (type == LogRecord::Type::Put && !getField(payload, pos, out.value)) return false;
    return pos == payload.size();  // no leftover bytes
}

}  // namespace kv::storage
