#include "common/crc32c.h"

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
