#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "storage/log_record.h"

namespace kv::server {

// What a Raft entry carries for the key-value state machine: an operation
// plus the identity of the request, so retries can be recognized.
//
// clientId 0 means "anonymous": no deduplication (used by tests and
// internal callers). A real client picks a random non-zero clientId and
// numbers its operations 1, 2, 3, ...; a retry reuses the same seq.
struct Command {
    std::uint64_t clientId = 0;
    std::uint64_t seq = 0;
    storage::LogRecord op;

    bool operator==(const Command&) const = default;
};

//   [client_id: u64][seq: u64][LogRecord payload to the end]
std::string encodeCommand(const Command& command);

// Returns false if the bytes are malformed.
bool decodeCommand(std::string_view bytes, Command& out);

}  // namespace kv::server
