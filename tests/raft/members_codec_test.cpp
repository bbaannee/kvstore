#include "raft/members_codec.h"

#include <gtest/gtest.h>

using namespace kv::raft;

TEST(MembersCodec, RoundTrips) {
    for (const Members& m : {Members{}, Members{{1, ""}}, Members{{1, "127.0.0.1:7001"}, {2, "host:7002"}, {9, ""}}}) {
        Members out;
        ASSERT_TRUE(decodeMembers(encodeMembers(m), out));
        EXPECT_EQ(out, m);
    }
}

TEST(MembersCodec, RejectsEveryTruncationAndTrailingBytes) {
    const std::string full = encodeMembers({{1, "a:1"}, {2, "b:2"}});
    Members out;
    for (std::size_t len = 0; len < full.size(); ++len)
        EXPECT_FALSE(decodeMembers(std::string_view(full).substr(0, len), out)) << len;
    EXPECT_FALSE(decodeMembers(full + "x", out));
}

// The rest of the code relies on a configuration being sorted and duplicate-free.
TEST(MembersCodec, RejectsUnsortedAndDuplicateMembers) {
    Members out;
    std::string unsorted;
    putMembers(unsorted, {{2, ""}, {1, ""}});
    EXPECT_FALSE(decodeMembers(unsorted, out));

    std::string duplicate;
    putMembers(duplicate, {{3, "x"}, {3, "y"}});
    EXPECT_FALSE(decodeMembers(duplicate, out));
}

TEST(MembersCodec, RejectsAbsurdCountsAndAddresses) {
    Members out;
    std::string huge;
    kv::putFixed32(huge, 0x7FFFFFFF);  // claims 2 billion members
    EXPECT_FALSE(decodeMembers(huge, out));

    std::string longAddress;
    kv::putFixed32(longAddress, 1);
    kv::putFixed64(longAddress, 1);
    kv::putFixed32(longAddress, kMaxAddressSize + 1);
    longAddress += std::string(kMaxAddressSize + 1, 'x');
    EXPECT_FALSE(decodeMembers(longAddress, out));
}

TEST(MembersCodec, SortedMembersOrdersById) {
    auto sorted = sortedMembers({{5, "e"}, {1, "a"}, {3, "c"}});
    ASSERT_EQ(sorted.size(), 3u);
    EXPECT_EQ(sorted[0].id, 1u);
    EXPECT_EQ(sorted[2].id, 5u);
}
