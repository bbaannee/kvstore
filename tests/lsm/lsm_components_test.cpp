// The building blocks of the LSM tree: Bloom filter, memtable, SSTable, cursors.

#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <iterator>
#include <map>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "lsm/bloom_filter.h"
#include "lsm/cursor.h"
#include "lsm/memtable.h"
#include "lsm/sstable.h"
#include "temp_dir.h"

using namespace kv::lsm;

namespace {

std::string key(int i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "key%06d", i);
    return buf;
}

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

}  // namespace

// ---------------------------------------------------------------------------
// Bloom filter
// ---------------------------------------------------------------------------

TEST(BloomFilter, NeverForgetsAKeyThatWasAdded) {
    std::vector<std::string> keys;
    for (int i = 0; i < 20000; ++i) keys.push_back(key(i));
    const std::string filter = BloomFilter::build(keys, 10);
    for (const auto& k : keys) ASSERT_TRUE(BloomFilter::mayContain(filter, k)) << k;  // no false negatives, ever
}

TEST(BloomFilter, RarelyClaimsAKeyThatWasNotAdded) {
    std::vector<std::string> keys;
    for (int i = 0; i < 10000; ++i) keys.push_back(key(i));
    const std::string filter = BloomFilter::build(keys, 10);

    int falsePositives = 0;
    for (int i = 100000; i < 120000; ++i) falsePositives += BloomFilter::mayContain(filter, key(i));
    const double rate = falsePositives / 20000.0;
    EXPECT_LT(rate, 0.03) << "10 bits per key should give about 1% false positives";
    EXPECT_GT(rate, 0.0) << "a filter that never errs is probably not a filter";

    // More bits per key, fewer mistakes.
    const std::string bigger = BloomFilter::build(keys, 20);
    int fewer = 0;
    for (int i = 100000; i < 120000; ++i) fewer += BloomFilter::mayContain(bigger, key(i));
    EXPECT_LT(fewer, falsePositives);
}

TEST(BloomFilter, EdgeCases) {
    const std::string empty = BloomFilter::build({}, 10);
    EXPECT_FALSE(BloomFilter::mayContain(empty, "anything"));  // nothing was added

    const std::string one = BloomFilter::build({"only"}, 10);
    EXPECT_TRUE(BloomFilter::mayContain(one, "only"));

    EXPECT_TRUE(BloomFilter::mayContain("", "x")) << "no usable filter: assume the key may be there";
    EXPECT_TRUE(BloomFilter::mayContain("x", "x"));
    std::string badK = one;
    badK.back() = static_cast<char>(99);  // an impossible number of hash functions
    EXPECT_TRUE(BloomFilter::mayContain(badK, "whatever"));
}

TEST(BloomFilter, HashesDifferBySeedAndInput) {
    EXPECT_NE(hash64("a", 1), hash64("a", 2));
    EXPECT_NE(hash64("a", 1), hash64("b", 1));
    EXPECT_EQ(hash64("a", 1), hash64("a", 1));
    EXPECT_NE(hash64("", 1), hash64("", 2));
}

// ---------------------------------------------------------------------------
// Memtable
// ---------------------------------------------------------------------------

TEST(MemTable, PutGetEraseOverwrite) {
    MemTable m;
    EXPECT_TRUE(m.empty());
    EXPECT_FALSE(m.get("a"));

    m.put("a", "1");
    m.put("b", "2");
    ASSERT_TRUE(m.get("a"));
    EXPECT_EQ(m.get("a")->value, "1");
    EXPECT_FALSE(m.get("a")->tombstone);

    m.put("a", "one");
    EXPECT_EQ(m.get("a")->value, "one");
    EXPECT_EQ(m.entries(), 2u);

    m.erase("a");  // a tombstone, not an absence: it has to hide older versions in deeper tables
    ASSERT_TRUE(m.get("a"));
    EXPECT_TRUE(m.get("a")->tombstone);
    m.erase("never-existed");
    EXPECT_TRUE(m.get("never-existed")->tombstone);
}

TEST(MemTable, SizeAccountingTracksOverwritesAndClear) {
    MemTable m;
    EXPECT_EQ(m.approximateBytes(), 0u);
    m.put("key", std::string(1000, 'x'));
    const auto big = m.approximateBytes();
    EXPECT_GT(big, 1000u);
    m.put("key", "small");
    EXPECT_LT(m.approximateBytes(), big);
    m.erase("key");
    const auto tomb = m.approximateBytes();
    EXPECT_LT(tomb, big);
    m.clear();
    EXPECT_EQ(m.approximateBytes(), 0u);
    EXPECT_TRUE(m.empty());
}

// ---------------------------------------------------------------------------
// SSTable
// ---------------------------------------------------------------------------

namespace {

// Writes entries 0, 2, 4, ... (so the odd ones are absent keys in between); every 7th is a tombstone.
TableInfo writeTable(const std::string& path, int count, std::size_t blockBytes = 256) {
    SstWriter w(path, blockBytes, 10, false);
    for (int i = 0; i < count; ++i) {
        const bool tomb = i % 7 == 3;
        w.add(key(i * 2), tomb, tomb ? "" : "value-" + std::to_string(i));
    }
    return w.finish();
}

}  // namespace

TEST(SSTable, RoundTripsManyEntriesAcrossManyBlocks) {
    kv::test::TempDir dir;
    const auto path = dir.file("t.sst");
    const TableInfo info = writeTable(path, 1000);
    EXPECT_EQ(info.entries, 1000u);
    EXPECT_EQ(info.smallest, key(0));
    EXPECT_EQ(info.largest, key(1998));
    EXPECT_GT(info.fileSize, 5000u);

    auto t = SstReader::open(path);
    EXPECT_EQ(t->entries(), 1000u);
    EXPECT_EQ(t->smallest(), key(0));
    EXPECT_EQ(t->largest(), key(1998));
    EXPECT_EQ(t->fileSize(), info.fileSize);

    for (int i = 0; i < 1000; ++i) {
        auto r = t->get(key(i * 2));
        ASSERT_TRUE(r) << key(i * 2);
        if (i % 7 == 3) {
            EXPECT_TRUE(r->tombstone);
        } else {
            EXPECT_FALSE(r->tombstone);
            EXPECT_EQ(r->value, "value-" + std::to_string(i));
        }
        EXPECT_FALSE(t->get(key(i * 2 + 1))) << "an absent key between two present ones";
    }
    EXPECT_FALSE(t->get("a")) << "before the smallest key";
    EXPECT_FALSE(t->get("zzz")) << "after the largest key";
}

TEST(SSTable, IteratorWalksInOrderAndSeeks) {
    kv::test::TempDir dir;
    const auto path = dir.file("t.sst");
    writeTable(path, 500);
    auto t = SstReader::open(path);

    SstReader::Iterator it(t.get());
    it.seekToFirst();
    int count = 0;
    std::string previous;
    for (; it.valid(); it.next(), ++count) {
        EXPECT_GT(it.key(), previous);
        previous = it.key();
        EXPECT_EQ(it.key(), key(count * 2));
    }
    EXPECT_EQ(count, 500);

    it.seek(key(101));  // between 100 and 102
    ASSERT_TRUE(it.valid());
    EXPECT_EQ(it.key(), key(102));
    it.seek(key(102));  // exact
    EXPECT_EQ(it.key(), key(102));
    it.seek("");        // before everything
    EXPECT_EQ(it.key(), key(0));
    it.seek(key(998));  // the last key
    ASSERT_TRUE(it.valid());
    it.next();
    EXPECT_FALSE(it.valid());
    it.seek(key(999));  // past the end
    EXPECT_FALSE(it.valid());
}

TEST(SSTable, BlockBoundariesAreNotSpecial) {
    kv::test::TempDir dir;
    // Blocks of 1 byte: every entry gets its own block.
    SstWriter w(dir.file("one.sst"), 1, 10, false);
    for (int i = 0; i < 50; ++i) w.add(key(i), false, "v" + std::to_string(i));
    w.finish();
    auto t = SstReader::open(dir.file("one.sst"));
    for (int i = 0; i < 50; ++i) EXPECT_EQ(t->get(key(i))->value, "v" + std::to_string(i));

    // A single entry in a single block, with an empty key and empty value.
    SstWriter w2(dir.file("tiny.sst"), 4096, 10, false);
    w2.add("", false, "");
    w2.finish();
    auto tiny = SstReader::open(dir.file("tiny.sst"));
    ASSERT_TRUE(tiny->get(""));
    EXPECT_EQ(tiny->get("")->value, "");
}

TEST(SSTable, WriterEnforcesItsRules) {
    kv::test::TempDir dir;
    {
        SstWriter w(dir.file("a.sst"), 256, 10, false);
        w.add("b", false, "1");
        EXPECT_THROW(w.add("a", false, "2"), std::logic_error);  // out of order
        EXPECT_THROW(w.add("b", false, "2"), std::logic_error);  // a duplicate
    }
    SstWriter empty(dir.file("e.sst"), 256, 10, false);
    EXPECT_THROW(empty.finish(), std::logic_error);  // an empty table has no smallest key
}

// Every byte of a table is covered by a checksum, so a single flipped bit anywhere must be noticed
// by opening the table and reading all of it.
TEST(SSTable, EverySingleBitFlipIsDetected) {
    kv::test::TempDir dir;
    const auto path = dir.file("t.sst");
    writeTable(path, 60, 128);
    const std::string good = readFile(path);

    auto touchEverything = [&] {
        auto t = SstReader::open(path);
        SstReader::Iterator it(t.get());
        for (it.seekToFirst(); it.valid(); it.next()) {}
        for (int i = 0; i < 60; ++i) t->get(key(i * 2));
    };
    ASSERT_NO_THROW(touchEverything());

    for (std::size_t i = 0; i < good.size(); ++i) {
        std::string bad = good;
        bad[i] = static_cast<char>(bad[i] ^ 0x01);
        writeFile(path, bad);
        EXPECT_THROW(touchEverything(), CorruptTableError) << "flipped bit in byte " << i << " of " << good.size();
    }
}

TEST(SSTable, EveryTruncationIsDetected) {
    kv::test::TempDir dir;
    const auto path = dir.file("t.sst");
    writeTable(path, 40, 128);
    const std::string good = readFile(path);
    for (std::size_t len = 0; len < good.size(); ++len) {
        writeFile(path, good.substr(0, len));
        EXPECT_THROW(SstReader::open(path), CorruptTableError) << "truncated to " << len;
    }
    writeFile(path, good + "trailing garbage");  // the footer must be at the very end
    EXPECT_THROW(SstReader::open(path), CorruptTableError);
}

TEST(SSTable, FilterLetsAbsentKeysSkipTheDisk) {
    kv::test::TempDir dir;
    const auto path = dir.file("t.sst");
    writeTable(path, 2000, 512);
    auto t = SstReader::open(path);

    // Corrupt a DATA block on disk behind the reader's back. Lookups of keys the filter rules out never
    // read data blocks, so they still work; a lookup that needs the block fails.
    std::string bytes = readFile(path);
    bytes[10] = static_cast<char>(bytes[10] ^ 0xFF);  // inside the first data block
    writeFile(path, bytes);

    int skipped = 0;
    for (int i = 0; i < 500; ++i) {
        try {
            if (!t->get(key(i * 2 + 1))) ++skipped;  // an absent key (and in the damaged first block's range, mostly)
        } catch (const CorruptTableError&) {
        }
    }
    EXPECT_GT(skipped, 450) << "the Bloom filter should answer almost all absent-key lookups without reading a block";
}

// ---------------------------------------------------------------------------
// Cursors
// ---------------------------------------------------------------------------

TEST(Cursor, MergingTakesTheNewestVersionOfEachKey) {
    kv::test::TempDir dir;
    // The newest source: a memtable.
    MemTable newest;
    newest.put("b", "new-b");
    newest.erase("c");  // deletes a key an older source has
    newest.put("e", "e");

    // An older table.
    SstWriter w(dir.file("old.sst"), 256, 10, false);
    w.add("a", false, "old-a");
    w.add("b", false, "old-b");
    w.add("c", false, "old-c");
    w.add("d", false, "old-d");
    w.finish();
    auto table = SstReader::open(dir.file("old.sst"));

    std::vector<std::unique_ptr<Cursor>> sources;
    sources.push_back(newMemTableCursor(newest));
    sources.push_back(newTableCursor(table));
    auto merged = newMergingCursor(std::move(sources));

    std::vector<std::string> seen;
    for (merged->seekToFirst(); merged->valid(); merged->next())
        seen.push_back(merged->key() + "=" + (merged->tombstone() ? "<deleted>" : merged->value()));
    EXPECT_EQ(seen, (std::vector<std::string>{"a=old-a", "b=new-b", "c=<deleted>", "d=old-d", "e=e"}));

    merged->seek("c");
    EXPECT_EQ(merged->key(), "c");
    merged->seek("cc");
    EXPECT_EQ(merged->key(), "d");
    merged->seek("zzz");
    EXPECT_FALSE(merged->valid());
}

TEST(Cursor, MergingManySourcesMatchesAModel) {
    kv::test::TempDir dir;
    std::mt19937_64 rng(5);
    std::map<std::string, std::pair<bool, std::string>> model;  // key -> (tombstone, value), the newest wins
    std::vector<std::shared_ptr<SstReader>> tables;

    // Five tables from oldest to newest, each a random subset; later ones overwrite earlier ones.
    for (int t = 0; t < 5; ++t) {
        std::map<std::string, std::pair<bool, std::string>> contents;
        for (int i = 0; i < 80; ++i) {
            const std::string k = key(static_cast<int>(rng() % 150));
            contents[k] = {rng() % 5 == 0, "t" + std::to_string(t) + "-" + k};
        }
        SstWriter w(dir.file(std::to_string(t) + ".sst"), 200, 10, false);
        for (const auto& [k, v] : contents) w.add(k, v.first, v.first ? "" : v.second);
        w.finish();
        tables.push_back(SstReader::open(dir.file(std::to_string(t) + ".sst")));
        for (const auto& [k, v] : contents) model[k] = {v.first, v.first ? "" : v.second};
    }

    std::vector<std::unique_ptr<Cursor>> sources;
    for (auto it = tables.rbegin(); it != tables.rend(); ++it) sources.push_back(newTableCursor(*it));  // newest first
    auto merged = newMergingCursor(std::move(sources));

    auto expected = model.begin();
    for (merged->seekToFirst(); merged->valid(); merged->next(), ++expected) {
        ASSERT_NE(expected, model.end());
        EXPECT_EQ(merged->key(), expected->first);
        EXPECT_EQ(merged->tombstone(), expected->second.first);
        EXPECT_EQ(merged->value(), expected->second.second);
    }
    EXPECT_EQ(expected, model.end());
}
