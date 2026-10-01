#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

namespace kv::storage {

// One entry in the write-ahead log: a single change to the store.
struct LogRecord {
    enum class Type : unsigned char { Put = 1, Del = 2 };

    Type type = Type::Put;
    std::string key;
    std::string value;  // always empty for Del

    static LogRecord put(std::string key, std::string value) {
        return {Type::Put, std::move(key), std::move(value)};
    }
    static LogRecord del(std::string key) { return {Type::Del, std::move(key), {}}; }

    bool operator==(const LogRecord&) const = default;
};

// Keys and values longer than this are rejected.
inline constexpr std::size_t kMaxFieldSize = 64 * 1024 * 1024;

// The record's contents as bytes (the "payload"). How payloads are framed
// and checksummed in the log file is in log_format.h.
//
//   [type: 1 byte][key_len: u32][key][value_len: u32][value]
//                                    └──── only for Put ────┘
std::string encodePayload(const LogRecord& rec);

// Parses a payload. Returns false if it's malformed: unknown type, a length
// that points past the end, or leftover bytes.
bool decodePayload(std::string_view payload, LogRecord& out);

}  // namespace kv::storage
