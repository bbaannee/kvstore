#include "common/coding.h"

#include <gtest/gtest.h>

TEST(Coding, Fixed32IsLittleEndian) {
    std::string buf;
    kv::putFixed32(buf, 0x12345678);
    ASSERT_EQ(buf.size(), 4u);
    EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x78);
    EXPECT_EQ(static_cast<unsigned char>(buf[1]), 0x56);
    EXPECT_EQ(static_cast<unsigned char>(buf[2]), 0x34);
    EXPECT_EQ(static_cast<unsigned char>(buf[3]), 0x12);
}

TEST(Coding, Fixed32RoundTrips) {
    for (std::uint32_t v : {0u, 1u, 255u, 256u, 0x7FFFFFFFu, 0xFFFFFFFFu}) {
        std::string buf;
        kv::putFixed32(buf, v);
        EXPECT_EQ(kv::decodeFixed32(buf.data()), v);
    }
}
