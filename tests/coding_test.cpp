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

TEST(Coding, Fixed64IsLittleEndianAndRoundTrips) {
    std::string buf;
    kv::putFixed64(buf, 0x0102030405060708ull);
    ASSERT_EQ(buf.size(), 8u);
    EXPECT_EQ(static_cast<unsigned char>(buf[0]), 0x08);
    EXPECT_EQ(static_cast<unsigned char>(buf[7]), 0x01);
    EXPECT_EQ(kv::decodeFixed64(buf.data()), 0x0102030405060708ull);

    for (std::uint64_t v : {0ull, 1ull, 0xFFFFFFFFull, 0x100000000ull, ~0ull}) {
        std::string b;
        kv::putFixed64(b, v);
        EXPECT_EQ(kv::decodeFixed64(b.data()), v);
    }
}

TEST(ByteReader, ReadsFieldsInOrder) {
    std::string buf;
    buf += '\x07';
    kv::putFixed32(buf, 42);
    kv::putFixed64(buf, 1ull << 40);
    kv::putFixed32(buf, 3);
    buf += "abc";

    kv::ByteReader r(buf);
    std::uint8_t a;
    std::uint32_t b;
    std::uint64_t c;
    std::string s;
    ASSERT_TRUE(r.getU8(a));
    ASSERT_TRUE(r.getFixed32(b));
    ASSERT_TRUE(r.getFixed64(c));
    ASSERT_TRUE(r.getLengthPrefixed(100, s));
    EXPECT_EQ(a, 7);
    EXPECT_EQ(b, 42u);
    EXPECT_EQ(c, 1ull << 40);
    EXPECT_EQ(s, "abc");
    EXPECT_TRUE(r.atEnd());
}

TEST(ByteReader, NeverReadsPastTheEnd) {
    std::string buf;
    kv::putFixed32(buf, 5);  // claims 5 bytes follow
    buf += "abc";            // only 3 do

    kv::ByteReader r(buf);
    std::string s;
    EXPECT_FALSE(r.getLengthPrefixed(100, s));

    std::uint64_t v;
    kv::ByteReader shortReader(std::string_view(buf).substr(0, 7));
    EXPECT_FALSE(shortReader.getFixed64(v));
}

TEST(ByteReader, RejectsOversizedLengthBeforeAllocating) {
    std::string buf;
    kv::putFixed32(buf, 0xFFFFFFFF);
    kv::ByteReader r(buf);
    std::string s;
    EXPECT_FALSE(r.getLengthPrefixed(1024, s));
}
