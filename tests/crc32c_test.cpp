#include "common/crc32c.h"

#include <random>
#include <string>

#include <gtest/gtest.h>

using kv::crc32c;

// Known answers from the CRC-32C specification (RFC 3720) and LevelDB.
TEST(Crc32c, StandardTestVectors) {
    EXPECT_EQ(crc32c(""), 0x00000000u);
    EXPECT_EQ(crc32c("123456789"), 0xE3069283u);
    EXPECT_EQ(crc32c(std::string(32, '\x00')), 0x8A9136AAu);
    EXPECT_EQ(crc32c(std::string(32, '\xFF')), 0x62A8AB43u);

    std::string ascending, descending;
    for (int i = 0; i < 32; ++i) {
        ascending += static_cast<char>(i);
        descending += static_cast<char>(31 - i);
    }
    EXPECT_EQ(crc32c(ascending), 0x46DD794Eu);
    EXPECT_EQ(crc32c(descending), 0x113FDB5Cu);
}

namespace {

// The definition of CRC-32C, one bit at a time. Slow and obviously correct:
// the fast implementation must agree with it on every input.
std::uint32_t crc32cBitwise(const std::string& data) {
    std::uint32_t crc = 0xFFFFFFFF;
    for (char ch : data) {
        crc ^= static_cast<unsigned char>(ch);
        for (int bit = 0; bit < 8; ++bit) crc = (crc & 1) ? (crc >> 1) ^ 0x82F63B78 : crc >> 1;
    }
    return crc ^ 0xFFFFFFFF;
}

}  // namespace

// The fast version processes 8 bytes at a time plus a byte-by-byte tail, so
// every length (and so every tail size) and every alignment matters.
TEST(Crc32c, FastImplementationMatchesTheDefinition) {
    std::mt19937_64 rng(7);
    std::string pool(600, '\0');
    for (char& c : pool) c = static_cast<char>(rng());

    for (std::size_t offset = 0; offset < 9; ++offset)  // unaligned starts
        for (std::size_t len = 0; len < 300; ++len) {
            std::string piece = pool.substr(offset, len);
            ASSERT_EQ(crc32c(piece), crc32cBitwise(piece)) << "offset " << offset << " length " << len;
        }
    std::string big(1'000'003, '\0');
    for (char& c : big) c = static_cast<char>(rng());
    EXPECT_EQ(crc32c(big), crc32cBitwise(big));
}

TEST(Crc32c, DetectsEverySingleBitFlip) {
    const std::string data = "PUT name Bane";
    const auto original = crc32c(data);
    for (std::size_t i = 0; i < data.size(); ++i) {
        for (int bit = 0; bit < 8; ++bit) {
            std::string damaged = data;
            damaged[i] = static_cast<char>(damaged[i] ^ (1 << bit));
            EXPECT_NE(crc32c(damaged), original) << "byte " << i << " bit " << bit;
        }
    }
}
