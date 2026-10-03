#pragma once

#include <cstddef>
#include <string>

#include "common/coding.h"
#include "raft/types.h"

namespace kv::raft {

// One log entry as bytes. Used both for the Raft log on disk and for
// AppendEntries messages on the network, so the two can't drift apart.
//
//   [term: u64][index: u64][type: u8][data_len: u32][data]

// An entry with empty data: term + index + type + data_len.
inline constexpr std::size_t kMinEncodedEntrySize = 8 + 8 + 1 + 4;

// Entries with more data than this are rejected when decoding.
inline constexpr std::size_t kMaxEntryDataSize = 64 * 1024 * 1024;

void putEntry(std::string& out, const Entry& entry);

// Returns false if the bytes are malformed. Never reads out of bounds.
bool getEntry(ByteReader& in, Entry& entry);

}  // namespace kv::raft
