#pragma once

#include <cstdint>
#include <string>

namespace kv {

// Fixed-width integers in little-endian byte order (lowest byte first).
//
// We build the bytes with shifts instead of memcpy'ing the integer, so the
// on-disk/on-wire format is the same no matter which CPU wrote it.

inline void putFixed32(std::string& dst, std::uint32_t v) {
    const char bytes[4] = {
        static_cast<char>(v & 0xFF),
        static_cast<char>((v >> 8) & 0xFF),
        static_cast<char>((v >> 16) & 0xFF),
        static_cast<char>((v >> 24) & 0xFF),
    };
    dst.append(bytes, 4);
}

// Reads 4 bytes starting at `p`. The caller guarantees they exist.
inline std::uint32_t decodeFixed32(const char* p) {
    const auto* b = reinterpret_cast<const unsigned char*>(p);
    return static_cast<std::uint32_t>(b[0]) |
           static_cast<std::uint32_t>(b[1]) << 8 |
           static_cast<std::uint32_t>(b[2]) << 16 |
           static_cast<std::uint32_t>(b[3]) << 24;
}

}  // namespace kv
