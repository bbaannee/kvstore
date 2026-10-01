#include "storage/log_record.h"

#include <vector>

#include <gtest/gtest.h>

using kv::storage::decodePayload;
using kv::storage::encodePayload;
using kv::storage::LogRecord;

TEST(LogRecord, PayloadLayout) {
    // [type=1][key_len=1,0,0,0]['k'][value_len=2,0,0,0]['v','w']
    EXPECT_EQ(encodePayload(LogRecord::put("k", "vw")), std::string("\x01\x01\0\0\0k\x02\0\0\0vw", 12));
    EXPECT_EQ(encodePayload(LogRecord::del("k")), std::string("\x02\x01\0\0\0k", 6));
}

TEST(LogRecord, RoundTripsAwkwardValues) {
    for (const auto& rec : std::vector<LogRecord>{
             LogRecord::put("k", "hello world"),
             LogRecord::put("k", "line1\nline2"),
             LogRecord::put("k", ""),
             LogRecord::put("", "empty key"),
             LogRecord::put("k", std::string("a\0b", 3)),
             LogRecord::del("k"),
         }) {
        LogRecord out;
        ASSERT_TRUE(decodePayload(encodePayload(rec), out));
        EXPECT_EQ(out, rec);
    }
}

TEST(LogRecord, RejectsEveryTruncatedPayload) {
    const std::string full = encodePayload(LogRecord::put("key", "some value"));
    for (std::size_t len = 0; len < full.size(); ++len) {
        LogRecord out;
        EXPECT_FALSE(decodePayload(full.substr(0, len), out)) << "length " << len;
    }
}

TEST(LogRecord, RejectsMalformedPayloads) {
    LogRecord out;
    EXPECT_FALSE(decodePayload(std::string("\x07\x01\0\0\0k", 6), out));        // unknown type
    EXPECT_FALSE(decodePayload(std::string("\x02\x01\0\0\0kX", 7), out));       // leftover byte
    EXPECT_FALSE(decodePayload(std::string("\x02\xFF\xFF\xFF\x7F", 5), out));   // huge key length
}
