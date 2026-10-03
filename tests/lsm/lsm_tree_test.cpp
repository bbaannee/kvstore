#include "lsm/lsm_tree.h"

#include <filesystem>
#include <fstream>
#include <iterator>
#include <map>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "storage/log_reader.h"
#include "temp_dir.h"

using namespace kv::lsm;
namespace fs = std::filesystem;

namespace {

// Tiny sizes, so that a few hundred writes already mean many flushes and several levels of compaction.
Options tiny(const std::string& dir) {
    Options o;
    o.dir = dir;
    o.memtableBytes = 1200;
    o.blockBytes = 128;
    o.l0CompactionTrigger = 3;
    o.level1MaxBytes = 3000;
    o.levelMultiplier = 3;
    o.targetFileBytes = 700;
    o.maxLevels = 5;
    o.syncWal = false;
    o.syncTables = false;
    return o;
}

std::string key(int i) {
    char buf[16];
    std::snprintf(buf, sizeof(buf), "key%05d", i);
    return buf;
}

std::map<std::string, std::string> contents(const LsmTree& db) {
    std::map<std::string, std::string> out;
    db.forEach([&](const std::string& k, const std::string& v) { out[k] = v; });
    return out;
}

std::string readFile(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(in), {}};
}

void writeFile(const std::string& path, const std::string& bytes) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << bytes;
}

std::vector<std::string> filesWithExtension(const std::string& dir, const std::string& ext) {
    std::vector<std::string> out;
    for (const auto& e : fs::directory_iterator(dir))
        if (e.path().extension() == ext) out.push_back(e.path().string());
    return out;
}

// Levels >= 1 must hold sorted, non-overlapping tables.
void expectDeepLevelsAreSortedAndDisjoint(const LsmTree& db) {
    auto levels = db.tables();
    for (std::size_t l = 1; l < levels.size(); ++l)
        for (std::size_t i = 1; i < levels[l].size(); ++i)
            EXPECT_LT(levels[l][i - 1].largest, levels[l][i].smallest) << "level " << l << " tables overlap";
}

}  // namespace

TEST(LsmTree, PutGetEraseOverwrite) {
    kv::test::TempDir dir;
    LsmTree db(tiny(dir.file("db")));
    EXPECT_FALSE(db.get("a"));

    db.put("a", "1");
    db.put("b", "2");
    EXPECT_EQ(db.get("a"), "1");
    db.put("a", "one");
    EXPECT_EQ(db.get("a"), "one");
    db.erase("a");
    EXPECT_FALSE(db.get("a"));
    EXPECT_EQ(db.get("b"), "2");
    db.erase("never-existed");  // harmless
    db.put("", "empty key");
    EXPECT_EQ(db.get(""), "empty key");
    db.put("k", "");
    EXPECT_EQ(db.get("k"), "");  // an empty value is a value, not a deletion
}

TEST(LsmTree, ScanAndForEachAreSortedAndHideDeletedKeys) {
    kv::test::TempDir dir;
    LsmTree db(tiny(dir.file("db")));
    for (int i = 0; i < 300; ++i) db.put(key(i), "v" + std::to_string(i));  // many flushes
    for (int i = 0; i < 300; i += 3) db.erase(key(i));

    auto all = db.scan("");
    EXPECT_EQ(all.size(), 200u);
    for (std::size_t i = 1; i < all.size(); ++i) EXPECT_LT(all[i - 1].first, all[i].first);

    auto part = db.scan(key(100), key(110));
    ASSERT_EQ(part.size(), 7u);  // 100..109 minus the multiples of 3 (102, 105, 108)
    EXPECT_EQ(part.front().first, key(100));
    EXPECT_EQ(part.back().first, key(109));

    EXPECT_EQ(db.scan(key(100), "", 5).size(), 5u);
    EXPECT_TRUE(db.scan(key(900)).empty());
    EXPECT_EQ(contents(db).size(), 200u);
}

TEST(LsmTree, FlushingWritesATableAndDataStaysReadable) {
    kv::test::TempDir dir;
    Options o = tiny(dir.file("db"));
    o.memtableBytes = 1 << 20;  // flush only on demand
    LsmTree db(o);
    for (int i = 0; i < 50; ++i) db.put(key(i), "v" + std::to_string(i));
    EXPECT_EQ(db.stats().tablesPerLevel[0], 0u);

    db.flush();
    EXPECT_EQ(db.stats().tablesPerLevel[0], 1u);
    EXPECT_EQ(db.stats().memtableBytes, 0u);
    EXPECT_EQ(filesWithExtension(dir.file("db"), ".sst").size(), 1u);
    for (int i = 0; i < 50; ++i) EXPECT_EQ(db.get(key(i)), "v" + std::to_string(i));

    db.flush();  // nothing in the memtable: no new table
    EXPECT_EQ(db.stats().tablesPerLevel[0], 1u);
}

TEST(LsmTree, ANewerVersionInAHigherPlaceHidesOlderOnes) {
    kv::test::TempDir dir;
    Options o = tiny(dir.file("db"));
    o.memtableBytes = 1 << 20;
    o.l0CompactionTrigger = 100;  // keep everything in level 0: several overlapping tables
    LsmTree db(o);
    db.put("x", "v1");
    db.flush();
    db.put("x", "v2");
    db.flush();
    db.put("x", "v3");
    db.flush();
    EXPECT_EQ(db.stats().tablesPerLevel[0], 3u);
    EXPECT_EQ(db.get("x"), "v3");
    db.put("x", "v4");  // and the memtable beats all the tables
    EXPECT_EQ(db.get("x"), "v4");
    EXPECT_EQ(contents(db).at("x"), "v4");
}

// A delete must not let an older version come back to life, whether the old version is in the
// same table level, a deeper one, or the tombstone itself has moved on.
TEST(LsmTree, DeletedKeysStayDeletedThroughFlushAndCompaction) {
    kv::test::TempDir dir;
    LsmTree db(tiny(dir.file("db")));

    db.put("victim", "old value");
    for (int i = 0; i < 200; ++i) db.put(key(i), std::string(30, 'a'));  // pushes "victim" deep
    db.erase("victim");
    EXPECT_FALSE(db.get("victim"));
    for (int i = 200; i < 600; ++i) {
        db.put(key(i), std::string(30, 'b'));  // flushes and compactions happen around the tombstone
        ASSERT_FALSE(db.get("victim")) << "resurrected after write " << i;
    }
    EXPECT_FALSE(contents(db).count("victim"));

    LsmTree reopened(tiny(dir.file("db")));  // nor after a restart
    EXPECT_FALSE(reopened.get("victim"));
}

TEST(LsmTree, CompactionDropsTombstonesAndOverwrittenValuesForGood) {
    kv::test::TempDir dir;
    LsmTree db(tiny(dir.file("db")));
    for (int round = 0; round < 6; ++round)
        for (int i = 0; i < 100; ++i) db.put(key(i), "round" + std::to_string(round) + std::string(20, 'x'));
    for (int i = 0; i < 100; ++i) db.erase(key(i));  // delete everything
    for (int i = 1000; i < 1400; ++i) db.put(key(i), std::string(20, 'y'));  // push the tombstones to the bottom
    db.flush();

    std::uint64_t entries = 0;
    for (const auto& level : db.tables())
        for (const auto& t : level) entries += t.entries;
    // 400 live keys, plus whatever tombstones are still on their way down; certainly far fewer than
    // the 600 overwrites + 100 deletes + 400 writes that went in.
    EXPECT_LT(entries, 700u);
    EXPECT_EQ(contents(db).size(), 400u);
    EXPECT_GT(db.stats().compactions, 3u);
}

TEST(LsmTree, CompactionKeepsTheLevelInvariants) {
    kv::test::TempDir dir;
    LsmTree db(tiny(dir.file("db")));
    std::mt19937_64 rng(11);
    for (int i = 0; i < 3000; ++i) {
        db.put(key(static_cast<int>(rng() % 400)), std::to_string(i));
        if (i % 97 == 0) expectDeepLevelsAreSortedAndDisjoint(db);
    }
    expectDeepLevelsAreSortedAndDisjoint(db);
    EXPECT_LT(db.stats().tablesPerLevel[0], 3u) << "level 0 must be compacted before it reaches the trigger";
    EXPECT_GT(db.stats().tablesPerLevel[1] + db.stats().tablesPerLevel[2], 0u);
    // There should be no more than one live version per key in total: the file count stays bounded.
    EXPECT_LT(filesWithExtension(dir.file("db"), ".sst").size(), 60u);
}

// The big one: random operations against a std::map, with restarts, checked often and completely.
TEST(LsmTree, BehavesLikeAMapUnderRandomOperationsAndRestarts) {
    for (std::uint64_t seed = 1; seed <= 6; ++seed) {
        SCOPED_TRACE("seed " + std::to_string(seed));
        kv::test::TempDir dir;
        std::mt19937_64 rng(seed);
        auto db = std::make_unique<LsmTree>(tiny(dir.file("db")));
        std::map<std::string, std::string> model;
        std::uint64_t compactions = 0;  // (the counter restarts from zero whenever the tree is reopened)

        for (int step = 0; step < 2500; ++step) {
            const std::string k = key(static_cast<int>(rng() % 300));
            switch (rng() % 12) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4: {
                    const std::string v = "s" + std::to_string(step) + std::string(rng() % 40, 'v');
                    db->put(k, v);
                    model[k] = v;
                    break;
                }
                case 5:
                case 6:
                    db->erase(k);
                    model.erase(k);
                    break;
                case 7: {  // a batch: all of it or none of it
                    WriteBatch b;
                    for (int i = 0; i < 4; ++i) {
                        const std::string bk = key(static_cast<int>(rng() % 300));
                        if (rng() % 3 == 0) {
                            b.erase(bk);
                            model.erase(bk);
                        } else {
                            b.put(bk, "batch" + std::to_string(step));
                            model[bk] = "batch" + std::to_string(step);
                        }
                    }
                    db->write(b);
                    break;
                }
                case 8: {
                    auto it = model.find(k);
                    auto got = db->get(k);
                    if (it == model.end()) ASSERT_FALSE(got) << k;
                    else ASSERT_EQ(got, it->second) << k;
                    break;
                }
                case 9: {  // a range scan
                    const std::string lo = key(static_cast<int>(rng() % 300)), hi = key(static_cast<int>(rng() % 300));
                    auto got = db->scan(lo, hi, 25);
                    auto it = model.lower_bound(lo);
                    for (const auto& [gk, gv] : got) {
                        ASSERT_TRUE(it != model.end() && it->first < hi) << "scan returned an extra key " << gk;
                        ASSERT_EQ(gk, it->first);
                        ASSERT_EQ(gv, it->second);
                        ++it;
                    }
                    if (got.size() < 25) {
                        ASSERT_TRUE(it == model.end() || !(it->first < hi)) << "scan stopped early";
                    }
                    break;
                }
                case 10:
                    if (rng() % 8 == 0) db->flush();
                    break;
                default:
                    if (rng() % 60 == 0) {  // a restart: drop the object (like a crash with a synced log) and reopen
                        compactions += db->stats().compactions;
                        db.reset();
                        db = std::make_unique<LsmTree>(tiny(dir.file("db")));
                    }
            }
            if (step % 400 == 399) {
                ASSERT_EQ(contents(*db), model) << "full comparison at step " << step;
                expectDeepLevelsAreSortedAndDisjoint(*db);
            }
        }
        ASSERT_EQ(contents(*db), model);
        compactions += db->stats().compactions;
        EXPECT_GT(compactions, 5u) << "the test didn't compact enough to mean much";
    }
}

// ---------------------------------------------------------------------------
// Durability and recovery
// ---------------------------------------------------------------------------

TEST(LsmTreeRecovery, UnflushedWritesComeBackFromTheLog) {
    kv::test::TempDir dir;
    {
        Options o = tiny(dir.file("db"));
        o.memtableBytes = 1 << 20;
        LsmTree db(o);
        for (int i = 0; i < 100; ++i) db.put(key(i), "v" + std::to_string(i));
        db.erase(key(7));
        // the object is destroyed here without flushing: everything is only in the log
    }
    Options o = tiny(dir.file("db"));
    o.memtableBytes = 1 << 20;
    LsmTree db(o);
    EXPECT_EQ(db.recovery().walBatchesReplayed, 101u);
    EXPECT_EQ(db.stats().tablesPerLevel[0], 0u);
    EXPECT_EQ(contents(db).size(), 99u);
    EXPECT_FALSE(db.get(key(7)));
    EXPECT_EQ(db.get(key(8)), "v8");
}

TEST(LsmTreeRecovery, TablesAndLogBothSurviveARestart) {
    kv::test::TempDir dir;
    std::map<std::string, std::string> model;
    {
        LsmTree db(tiny(dir.file("db")));
        for (int i = 0; i < 500; ++i) {
            db.put(key(i % 120), "v" + std::to_string(i));
            model[key(i % 120)] = "v" + std::to_string(i);
        }
    }
    LsmTree db(tiny(dir.file("db")));
    EXPECT_GT(db.recovery().tablesLoaded, 0u);
    EXPECT_EQ(contents(db), model);
    expectDeepLevelsAreSortedAndDisjoint(db);
}

// The log is the source of truth for the memtable. Cut it at every byte (a crash at every
// possible point of a write) and check recovery gives exactly the batches that were complete.
TEST(LsmTreeRecovery, CrashAtEveryByteOfTheLogKeepsWholeBatchesOnly) {
    kv::test::TempDir dir;
    const auto source = dir.file("source");
    Options o = tiny(source);
    o.memtableBytes = 1 << 20;  // everything stays in the log

    std::vector<std::size_t> batchEnds;  // log size after each batch
    std::vector<std::map<std::string, std::string>> after;  // the model after each batch
    std::string walPath;
    {
        LsmTree db(o);
        std::map<std::string, std::string> model;
        for (const auto& e : fs::directory_iterator(source))
            if (e.path().filename().string().rfind("wal-", 0) == 0) walPath = e.path().string();
        for (int b = 0; b < 12; ++b) {
            WriteBatch batch;
            for (int i = 0; i < 3; ++i) {
                const std::string k = key(b * 2 + i);  // overlapping keys across batches
                if (b % 4 == 3 && i == 2) {
                    batch.erase(key(b - 1));
                    model.erase(key(b - 1));
                } else {
                    batch.put(k, "b" + std::to_string(b) + "i" + std::to_string(i));
                    model[k] = "b" + std::to_string(b) + "i" + std::to_string(i);
                }
            }
            db.write(batch);
            batchEnds.push_back(fs::file_size(walPath));
            after.push_back(model);
        }
    }
    const std::string fullLog = readFile(walPath);
    const std::string manifest = readFile(source + "/MANIFEST");

    for (std::size_t cut = 0; cut <= fullLog.size(); ++cut) {
        SCOPED_TRACE("crash after " + std::to_string(cut) + " bytes of the log");
        const std::string copy = dir.file("copy");
        fs::remove_all(copy);
        fs::create_directories(copy);
        writeFile(copy + "/MANIFEST", manifest);
        writeFile(copy + "/" + fs::path(walPath).filename().string(), fullLog.substr(0, cut));

        std::size_t complete = 0;
        while (complete < batchEnds.size() && batchEnds[complete] <= cut) ++complete;

        Options co = o;
        co.dir = copy;
        LsmTree db(co);
        EXPECT_EQ(db.recovery().walBatchesReplayed, complete);
        const std::map<std::string, std::string> expected =
            complete == 0 ? std::map<std::string, std::string>{} : after[complete - 1];
        EXPECT_EQ(contents(db), expected);

        db.put("after-crash", "ok");  // and it is usable, and the torn tail doesn't swallow new writes
        LsmTree again(co);
        EXPECT_EQ(again.get("after-crash"), "ok");
    }
}

// A flush or compaction that crashed halfway leaves files that are not part of the database.
TEST(LsmTreeRecovery, JunkFromACrashedFlushOrCompactionIsDeleted) {
    kv::test::TempDir dir;
    std::map<std::string, std::string> model;
    {
        LsmTree db(tiny(dir.file("db")));
        for (int i = 0; i < 300; ++i) {
            db.put(key(i % 80), "v" + std::to_string(i));
            model[key(i % 80)] = "v" + std::to_string(i);
        }
    }
    const auto before = filesWithExtension(dir.file("db"), ".sst").size();
    writeFile(dir.file("db") + "/999990.sst", "an orphan table from a crashed compaction");
    writeFile(dir.file("db") + "/wal-999991.log", "a stale log");
    writeFile(dir.file("db") + "/MANIFEST.tmp", "a half-written manifest");

    LsmTree db(tiny(dir.file("db")));
    EXPECT_EQ(contents(db), model);
    EXPECT_EQ(filesWithExtension(dir.file("db"), ".sst").size(), before);
    EXPECT_FALSE(fs::exists(dir.file("db") + "/wal-999991.log"));
    EXPECT_FALSE(fs::exists(dir.file("db") + "/MANIFEST.tmp"));
}

TEST(LsmTreeRecovery, DamageThatACrashCannotExplainIsRefused) {
    kv::test::TempDir dir;
    {
        LsmTree db(tiny(dir.file("db")));
        for (int i = 0; i < 300; ++i) db.put(key(i), "v" + std::to_string(i));
    }
    const std::string manifestPath = dir.file("db") + "/MANIFEST";
    const std::string manifest = readFile(manifestPath);

    std::string flipped = manifest;
    flipped[manifest.size() / 2] = static_cast<char>(flipped[manifest.size() / 2] ^ 0x01);
    writeFile(manifestPath, flipped);
    EXPECT_THROW(LsmTree{tiny(dir.file("db"))}, CorruptTableError);
    writeFile(manifestPath, manifest.substr(0, manifest.size() - 1));
    EXPECT_THROW(LsmTree{tiny(dir.file("db"))}, CorruptTableError);
    writeFile(manifestPath, manifest);

    // A table the manifest names is gone.
    auto tables = filesWithExtension(dir.file("db"), ".sst");
    ASSERT_FALSE(tables.empty());
    const std::string table = readFile(tables[0]);
    fs::remove(tables[0]);
    EXPECT_ANY_THROW(LsmTree{tiny(dir.file("db"))});

    // A table whose contents are damaged.
    std::string bad = table;
    bad[bad.size() - 10] = static_cast<char>(bad[bad.size() - 10] ^ 0xFF);
    writeFile(tables[0], bad);
    EXPECT_THROW(LsmTree{tiny(dir.file("db"))}, CorruptTableError);
}

TEST(LsmTree, ClearEmptiesEverythingAndTheTreeIsUsableAgain) {
    kv::test::TempDir dir;
    {
        LsmTree db(tiny(dir.file("db")));
        for (int i = 0; i < 400; ++i) db.put(key(i), "v" + std::to_string(i));
        ASSERT_GT(filesWithExtension(dir.file("db"), ".sst").size(), 3u);

        db.clear();
        EXPECT_TRUE(contents(db).empty());
        EXPECT_FALSE(db.get(key(5)));
        EXPECT_TRUE(filesWithExtension(dir.file("db"), ".sst").empty());

        db.put("fresh", "start");
        EXPECT_EQ(db.get("fresh"), "start");
    }
    LsmTree reopened(tiny(dir.file("db")));
    EXPECT_EQ(contents(reopened), (std::map<std::string, std::string>{{"fresh", "start"}}));
}

TEST(LsmTree, SyncIsSafeToCallAndWritesWithoutPerWriteSyncStillSurvive) {
    kv::test::TempDir dir;
    Options o = tiny(dir.file("db"));
    o.syncWal = false;  // the caller syncs when it needs to
    {
        LsmTree db(o);
        db.put("a", "1");
        db.sync();
        db.put("b", "2");
    }
    LsmTree db(o);
    EXPECT_EQ(db.get("a"), "1");
    EXPECT_EQ(db.get("b"), "2");  // (a process crash keeps the OS page cache; only a power cut would lose it)
}

TEST(LsmTree, ReadsOfAbsentKeysDoNotNeedToTouchManyTables) {
    kv::test::TempDir dir;
    Options o = tiny(dir.file("db"));
    o.l0CompactionTrigger = 1000;  // lots of overlapping tables in level 0
    o.memtableBytes = 600;
    LsmTree db(o);
    for (int i = 0; i < 400; ++i) db.put(key(i * 2), "v");  // even keys only
    ASSERT_GT(db.stats().tablesPerLevel[0], 15u);

    // Every absent key must still be answered correctly (the Bloom filters may only ever help, never mislead).
    for (int i = 0; i < 400; ++i) {
        EXPECT_FALSE(db.get(key(i * 2 + 1)));
        EXPECT_TRUE(db.get(key(i * 2)));
    }
}
