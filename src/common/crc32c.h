#pragma once

#include <cstdint>
#include <string_view>

namespace kv {

// CRC-32C (Castagnoli) checksum of `data`.
//
// A 4-byte fingerprint of the data: store it next to the data when writing,
// recompute it when reading, and if the two differ the data was damaged.
// Detects every single-bit error and every error burst up to 32 bits.
// Same checksum as ext4, iSCSI, LevelDB and RocksDB use.
std::uint32_t crc32c(std::string_view data);

}  // namespace kv
