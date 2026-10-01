#include "storage/log_reader.h"

#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/log_format.h"
#include "temp_dir.h"

using kv::storage::encodePayload;
using kv::storage::frameRecord;
using kv::storage::LogReader;
using kv::storage::LogRecord;
using kv::storage::ReadStatus;

namespace {

std::string frame(const LogRecord& rec) { return frameRecord(encodePayload(rec)); }

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

// Reads records until the first non-Ok status, which is returned.
ReadStatus readAll(LogReader& reader, std::vector<LogRecord>& out) {
    LogRecord rec;
    ReadStatus status;
    while ((status = reader.next(rec)) == ReadStatus::Ok) out.push_back(rec);
    return status;
}

const std::vector<LogRecord> kRecords = {
    LogRecord::put("a", "1"),
    LogRecord::put("b", "two words"),
    LogRecord::del("a"),
};

std::string allFramed() {
    std::string bytes;
    for (const auto& r : kRecords) bytes += frame(r);
    return bytes;
}

}  // namespace

TEST(LogReader, MissingFileIsEmpty) {
    kv::test::TempDir dir;
    LogReader reader(dir.file("nope.log"));
    LogRecord rec;
    EXPECT_EQ(reader.next(rec), ReadStatus::End);
    EXPECT_EQ(reader.validBytes(), 0u);
}

TEST(LogReader, ReadsAllRecords) {
    kv::test::TempDir dir;
    writeFile(dir.file("log"), allFramed());
    LogReader reader(dir.file("log"));
    std::vector<LogRecord> got;
    EXPECT_EQ(readAll(reader, got), ReadStatus::End);
    EXPECT_EQ(got, kRecords);
    EXPECT_EQ(reader.validBytes(), allFramed().size());
}

TEST(LogReader, ZeroFilledTailIsTorn) {
    kv::test::TempDir dir;
    writeFile(dir.file("log"), allFramed() + std::string(100, '\0'));
    LogReader reader(dir.file("log"));
    std::vector<LogRecord> got;
    EXPECT_EQ(readAll(reader, got), ReadStatus::Torn);
    EXPECT_EQ(got, kRecords);
    EXPECT_EQ(reader.validBytes(), allFramed().size());
}

TEST(LogReader, LastRecordWithBadPayloadIsTorn) {
    kv::test::TempDir dir;
    std::string bytes = allFramed();
    bytes.back() = static_cast<char>(bytes.back() ^ 0x01);
    writeFile(dir.file("log"), bytes);

    LogReader reader(dir.file("log"));
    std::vector<LogRecord> got;
    EXPECT_EQ(readAll(reader, got), ReadStatus::Torn);
    EXPECT_EQ(got.size(), kRecords.size() - 1);
}

// Flip one bit in every byte of every record except the last one. A crash
// can't cause any of these, so every single one must be reported as corrupt,
// never treated as a torn tail (which would delete the good records after it).
TEST(LogReader, BitFlipBeforeTheLastRecordIsCorruption) {
    kv::test::TempDir dir;
    const std::string good = allFramed();
    const std::size_t lastRecordStart = good.size() - frame(kRecords.back()).size();

    for (std::size_t i = 0; i < lastRecordStart; ++i) {
        SCOPED_TRACE("flipped byte " + std::to_string(i));
        std::string bytes = good;
        bytes[i] = static_cast<char>(bytes[i] ^ 0x01);
        writeFile(dir.file("log"), bytes);

        LogReader reader(dir.file("log"));
        std::vector<LogRecord> got;
        EXPECT_EQ(readAll(reader, got), ReadStatus::Corrupt);
        EXPECT_FALSE(reader.error().empty());
    }
}
