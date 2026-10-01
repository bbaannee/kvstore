#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "storage/log_record.h"

namespace kv::storage {

// How one record is laid out in the log file (all integers u32 little-endian):
//
//   offset  size  field
//   0       4     length        payload size in bytes
//   4       4     length_crc    crc32c of the 4 length bytes
//   8       4     payload_crc   crc32c of the payload
//   12      N     payload       see encodePayload()
//
// Why the length has its own checksum: if a bit flips in the length of a
// record in the middle of the log, the reader would think the record runs
// past the end of the file. That looks exactly like a crash mid-write, and
// "fixing" it by truncating would delete every good record after it. With
// length_crc, a length that passes the check is the real one.

inline constexpr std::size_t kHeaderSize = 12;

// Largest payload a valid record can have (type + two lengths + two fields).
inline constexpr std::size_t kMaxPayloadSize = 1 + 4 + kMaxFieldSize + 4 + kMaxFieldSize;

// Header + payload, ready to append to the log.
std::string frameRecord(std::string_view payload);

}  // namespace kv::storage
