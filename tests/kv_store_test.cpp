#include "storage/kv_store.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/log_record.h"
#include "temp_dir.h"

using kv::storage::KvStore;
using kv::storage::LogRecord;

namespace {

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

}  // namespace

TEST(KvStore, PutGetDel) {
    kv::test::TempDir dir;
    KvStore store(dir.file("data.log"));

    EXPECT_EQ(store.get("a"), std::nullopt);
    store.put("a", "1");
    EXPECT_EQ(store.get("a"), "1");
    store.put("a", "2");
    EXPECT_EQ(store.get("a"), "2");
    EXPECT_TRUE(store.del("a"));
    EXPECT_EQ(store.get("a"), std::nullopt);
    EXPECT_FALSE(store.del("a"));
}

TEST(KvStore, SurvivesReopen) {
    kv::test::TempDir dir;
    const auto log = dir.file("data.log");
    {
        KvStore store(log);
        store.put("name", "Bane");
        store.put("msg", "hello world");
        store.put("tmp", "x");
        store.del("tmp");
    }
    KvStore store(log);
    EXPECT_EQ(store.recovery().recordsReplayed, 4u);
    EXPECT_FALSE(store.recovery().truncatedTornTail);
    EXPECT_EQ(store.get("name"), "Bane");
    EXPECT_EQ(store.get("msg"), "hello world");
    EXPECT_EQ(store.get("tmp"), std::nullopt);
    EXPECT_EQ(store.size(), 2u);
}

TEST(KvStore, RefusesToOpenCorruptedLog) {
    kv::test::TempDir dir;
    const auto log = dir.file("data.log");
    {
        KvStore store(log);
        store.put("a", "1");
        store.put("b", "2");
    }
    std::string bytes = readFile(log);
    bytes[14] = static_cast<char>(bytes[14] ^ 0x01);  // inside the first record's payload
    writeFile(log, bytes);

    EXPECT_THROW(KvStore{log}, kv::storage::CorruptLogError);
    EXPECT_EQ(readFile(log), bytes);  // nothing was truncated or "repaired"
}

TEST(KvStore, DeletingMissingKeyWritesNothing) {
    kv::test::TempDir dir;
    const auto log = dir.file("data.log");
    KvStore store(log);
    EXPECT_FALSE(store.del("nope"));
    EXPECT_EQ(std::filesystem::file_size(log), 0u);
}

// The core crash-safety test. For every possible crash point in the log:
//   1. reopening keeps exactly the records that were fully written,
//   2. the torn tail is cut off,
//   3. a new write after recovery survives the next reopen.
TEST(KvStore, RecoversFromCrashAtEveryByte) {
    kv::test::TempDir dir;
    const auto source = dir.file("source.log");

    const std::vector<LogRecord> records = {
        LogRecord::put("a", "1"),
        LogRecord::put("b", "two words"),
        LogRecord::del("a"),
        LogRecord::put("c", "line\nbreak"),
    };
    std::vector<std::size_t> recordEnds;  // byte offset where each record ends
    {
        KvStore store(source);
        for (const auto& r : records) {
            if (r.type == LogRecord::Type::Put) store.put(r.key, r.value);
            else store.del(r.key);
            recordEnds.push_back(std::filesystem::file_size(source));
        }
    }
    const std::string full = readFile(source);

    for (std::size_t cut = 0; cut <= full.size(); ++cut) {
        SCOPED_TRACE("crash after " + std::to_string(cut) + " bytes");
        const auto log = dir.file("crash.log");
        writeFile(log, full.substr(0, cut));

        // What the store should contain: only records that fully fit.
        std::map<std::string, std::string> expected;
        std::size_t complete = 0;
        while (complete < records.size() && recordEnds[complete] <= cut) {
            const auto& r = records[complete++];
            if (r.type == LogRecord::Type::Put) expected[r.key] = r.value;
            else expected.erase(r.key);
        }
        const std::size_t goodBytes = complete ? recordEnds[complete - 1] : 0;

        {
            KvStore store(log);
            EXPECT_EQ(store.recovery().recordsReplayed, complete);
            EXPECT_EQ(store.recovery().truncatedTornTail, cut != goodBytes);
            EXPECT_EQ(std::filesystem::file_size(log), goodBytes);
            EXPECT_EQ(store.size(), expected.size());
            for (const auto& [k, v] : expected) EXPECT_EQ(store.get(k), v);

            store.put("after", "crash");
        }
        KvStore reopened(log);
        EXPECT_EQ(reopened.get("after"), "crash");
        EXPECT_FALSE(reopened.recovery().truncatedTornTail);
    }
}
