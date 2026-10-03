#include "common/crc32c.h"

#include <array>

#include "common/coding.h"

namespace kv {

namespace {

// The Castagnoli polynomial, bit-reversed because we process the lowest
// bit first.
constexpr std::uint32_t kPolynomial = 0x82F63B78;

// Slicing-by-8. Plain table-driven CRC handles one byte per step, each step
// depending on the previous one. Eight tables let us handle 8 bytes per
// step with independent lookups the CPU can run in parallel:
//   table[0][b]     = effect of byte value b on the CRC (the classic table)
//   table[k][b]     = effect of byte b followed by k zero bytes
// So the 8 bytes of a chunk can each be looked up in the table matching its
// distance from the end of the chunk, and XORed together. Computed at
// compile time.
using Tables = std::array<std::array<std::uint32_t, 256>, 8>;

constexpr Tables makeTables() {
    Tables t{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit) crc = (crc & 1) ? (crc >> 1) ^ kPolynomial : crc >> 1;
        t[0][i] = crc;
    }
    for (std::size_t k = 1; k < 8; ++k)
        for (std::uint32_t i = 0; i < 256; ++i) t[k][i] = (t[k - 1][i] >> 8) ^ t[0][t[k - 1][i] & 0xFF];
    return t;
}

constexpr Tables kTables = makeTables();

}  // namespace

std::uint32_t crc32c(std::string_view data) {
    std::uint32_t crc = 0xFFFFFFFF;  // standard initial value
    const char* p = data.data();
    std::size_t n = data.size();

    while (n >= 8) {  // 8 bytes at a time
        const std::uint32_t lo = decodeFixed32(p) ^ crc;
        const std::uint32_t hi = decodeFixed32(p + 4);
        crc = kTables[7][lo & 0xFF] ^ kTables[6][(lo >> 8) & 0xFF] ^ kTables[5][(lo >> 16) & 0xFF] ^
              kTables[4][lo >> 24] ^ kTables[3][hi & 0xFF] ^ kTables[2][(hi >> 8) & 0xFF] ^
              kTables[1][(hi >> 16) & 0xFF] ^ kTables[0][hi >> 24];
        p += 8;
        n -= 8;
    }
    while (n-- > 0) {  // the last few bytes, one at a time
        crc = kTables[0][(crc ^ static_cast<unsigned char>(*p++)) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;  // standard final inversion
}

}  // namespace kv
