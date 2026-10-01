#include "common/crc32c.h"

#include <array>

namespace kv {

namespace {

// The Castagnoli polynomial, bit-reversed because we process the lowest
// bit first.
constexpr std::uint32_t kPolynomial = 0x82F63B78;

// table[b] = the CRC contribution of byte value b, so we can process a whole
// byte per step instead of one bit at a time. Computed at compile time.
constexpr std::array<std::uint32_t, 256> makeTable() {
    std::array<std::uint32_t, 256> table{};
    for (std::uint32_t i = 0; i < 256; ++i) {
        std::uint32_t crc = i;
        for (int bit = 0; bit < 8; ++bit)
            crc = (crc & 1) ? (crc >> 1) ^ kPolynomial : crc >> 1;
        table[i] = crc;
    }
    return table;
}

constexpr auto kTable = makeTable();

}  // namespace

std::uint32_t crc32c(std::string_view data) {
    std::uint32_t crc = 0xFFFFFFFF;  // standard initial value
    for (char ch : data) {
        auto byte = static_cast<unsigned char>(ch);
        crc = kTable[(crc ^ byte) & 0xFF] ^ (crc >> 8);
    }
    return crc ^ 0xFFFFFFFF;  // standard final inversion
}

}  // namespace kv
