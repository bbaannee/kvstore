#pragma once

#include <cstddef>
#include <string>
#include <string_view>

#include "common/coding.h"
#include "raft/types.h"

namespace kv::raft {

// Members as bytes, used in Config log entries, snapshots, and the
// InstallSnapshot message.
//
//   [count: u32] then per member: [id: u64][address: u32 length + bytes]

// A member list can't be longer than this (decoding rejects bigger counts).
inline constexpr std::size_t kMaxMembers = 1024;
inline constexpr std::size_t kMaxAddressSize = 256;

void putMembers(std::string& out, const Members& members);

// Returns false if malformed. Also rejects lists that aren't sorted by id or
// contain duplicates, because the rest of the code relies on that.
bool getMembers(ByteReader& in, Members& members);

std::string encodeMembers(const Members& members);
bool decodeMembers(std::string_view bytes, Members& members);  // must consume all of `bytes`

// Sorted-by-id copy, as the configuration type requires.
Members sortedMembers(Members members);

}  // namespace kv::raft
