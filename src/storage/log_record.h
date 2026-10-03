#pragma once

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>

namespace kv::storage {

// One operation on the store. KvStore writes Put and Del to its write-ahead
// log. In a replicated cluster the same bytes are the command carried by a
// Raft entry; there, reads are commands too (Get), so they are ordered with
// the writes. KvStore never logs a Get.
struct LogRecord {
    enum class Type : unsigned char { Put = 1, Del = 2, Get = 3 };

    Type type = Type::Put;
    std::string key;
    std::string value;  // always empty for Del and Get

    static LogRecord put(std::string key, std::string value) {
        return {Type::Put, std::move(key), std::move(value)};
    }
    static LogRecord del(std::string key) { return {Type::Del, std::move(key), {}}; }
    static LogRecord get(std::string key) { return {Type::Get, std::move(key), {}}; }

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
