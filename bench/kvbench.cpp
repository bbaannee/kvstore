// kvbench: measurements for the pieces of the store.
//
//   kvbench wal        what an fsync costs, and what group commit saves
//   kvbench lsm        the LSM tree: writes, reads, Bloom filters, write amplification
//   kvbench cluster    a 3-server cluster over TCP: throughput and latency percentiles
//   kvbench            all of them          (add --quick for a shorter run; --dir PATH for where the files go)
//
// Everything runs on one machine over loopback, so absolute numbers say little about a real
// deployment. What the numbers are good for is COMPARING configurations: with and without fsync,
// with and without group commit, memory engine against LSM, ReadIndex reads against reads
// through the log, Bloom filter against none.

#include <sys/vfs.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "client/client.h"
#include "lsm/lsm_tree.h"
#include "server/node_server.h"
#include "storage/log_writer.h"

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
namespace fs = std::filesystem;

namespace {

bool g_quick = false;
fs::path g_root = fs::temp_directory_path();  // where the benchmarks write their files (see --dir)

// tmpfs lives in RAM, so an fsync there costs nothing and tells you nothing about durability.
bool onTmpfs(const fs::path& dir) {
    struct statfs fsInfo;
    return ::statfs(dir.c_str(), &fsInfo) == 0 && fsInfo.f_type == 0x01021994;
}

double secondsSince(Clock::time_point t) { return std::chrono::duration<double>(Clock::now() - t).count(); }

std::string zeroPadded(std::uint64_t n, int width = 10) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "key%0*llu", width, static_cast<unsigned long long>(n));
    return buf;
}

// A fresh scratch directory, deleted at the end.
class Scratch {
public:
    Scratch() {
        static int counter = 0;
        path_ = g_root / ("kvbench-" + std::to_string(::getpid()) + "-" + std::to_string(counter++));
        fs::remove_all(path_);
        fs::create_directories(path_);
    }
    ~Scratch() { fs::remove_all(path_); }
    std::string file(const std::string& name) const { return (path_ / name).string(); }

private:
    fs::path path_;
};

struct Latencies {
    std::vector<double> ms;
    void merge(const Latencies& other) { ms.insert(ms.end(), other.ms.begin(), other.ms.end()); }
    double percentile(double p) {
        if (ms.empty()) return 0;
        std::sort(ms.begin(), ms.end());
        return ms[std::min(ms.size() - 1, static_cast<std::size_t>(p / 100.0 * static_cast<double>(ms.size())))];
    }
};

// ---------------------------------------------------------------------------
// WAL: the price of durability
// ---------------------------------------------------------------------------

void benchWal() {
    std::printf("\n## Write-ahead log: what an fsync costs\n\n");
    std::printf("Appending 100-byte records to the checksummed log. fsync is what makes a record survive a power cut;\n");
    std::printf("group commit pays for one fsync per batch of records instead of one per record.\n\n");
    std::printf("| fsync policy | records/s | us/record |\n|---|---|---|\n");

    struct Case {
        const char* name;
        int batch;  // records per fsync; 0 = never
        int records;
    };
    const int scale = g_quick ? 1 : 4;
    for (const Case& c : {Case{"every record", 1, 300 * scale}, Case{"every 10 records", 10, 3000 * scale},
                          Case{"every 100 records", 100, 30000 * scale}, Case{"never (page cache only)", 0, 200000 * scale}}) {
        Scratch dir;
        kv::storage::LogWriter wal(dir.file("wal"), /*syncOnAppend=*/false);
        const std::string payload(100, 'x');
        auto start = Clock::now();
        for (int i = 1; i <= c.records; ++i) {
            wal.appendPayload(payload);
            if (c.batch > 0 && i % c.batch == 0) wal.sync();
        }
        if (c.batch > 0) wal.sync();
        const double s = secondsSince(start);
        std::printf("| %s | %.0f | %.1f |\n", c.name, c.records / s, s / c.records * 1e6);
    }
}

// ---------------------------------------------------------------------------
// LSM tree
// ---------------------------------------------------------------------------

kv::lsm::Options lsmOptions(const std::string& dir, std::size_t memtableBytes, int bloomBits) {
    kv::lsm::Options o;
    o.dir = dir;
    o.memtableBytes = memtableBytes;
    o.bloomBitsPerKey = bloomBits;
    o.syncWal = false;
    o.syncTables = false;
    return o;
}

void benchLsm() {
    std::printf("\n## LSM tree\n\n");
    const std::uint64_t n = g_quick ? 100'000 : 400'000;
    const std::size_t valueBytes = 100;
    const std::string value(valueBytes, 'v');

    std::printf("### Writes (%llu puts of %zu-byte values)\n\n", static_cast<unsigned long long>(n), valueBytes);
    std::printf("| workload | memtable | puts/s | MB/s | flushes | compactions | table write amplification |\n|---|---|---|---|---|---|---|\n");
    for (bool random : {false, true}) {
        for (std::size_t memtable : {std::size_t(1) << 20, std::size_t(4) << 20, std::size_t(16) << 20}) {
            Scratch dir;
            kv::lsm::LsmTree db(lsmOptions(dir.file("db"), memtable, 10));
            std::mt19937_64 rng(1);
            auto start = Clock::now();
            for (std::uint64_t i = 0; i < n; ++i) db.put(zeroPadded(random ? rng() % (n * 10) : i), value);
            db.flush();
            const double s = secondsSince(start);
            const auto st = db.stats();
            const double userBytes = static_cast<double>(n) * (13 + valueBytes);
            std::printf("| %s | %zu MB | %.0f | %.1f | %llu | %llu | %.1fx |\n", random ? "random keys" : "sequential keys",
                        memtable >> 20, n / s, userBytes / s / 1e6, static_cast<unsigned long long>(st.flushes),
                        static_cast<unsigned long long>(st.compactions),
                        static_cast<double>(st.bytesWrittenByFlush + st.bytesWrittenByCompaction) / userBytes);
        }
    }

    // Reads: build a database with many overlapping level-0 tables, so that finding (or not finding) a key
    // means asking many tables. The Bloom filter's job is to answer most of those questions from memory.
    std::printf("\n### Reads: the Bloom filter\n\n");
    std::printf("The database has many small tables that all overlap. A key that is NOT there has to be ruled out in\n");
    std::printf("every one of them. 'no filter' builds each table with a filter that says \"maybe\" to everything.\n\n");
    std::printf("| bloom filter | lookups of present keys/s | lookups of ABSENT keys/s |\n|---|---|---|\n");
    const std::uint64_t keys = g_quick ? 60'000 : 150'000;
    for (int bits : {10, 0}) {
        Scratch dir;
        auto o = lsmOptions(dir.file("db"), 256 * 1024, bits);
        o.l0CompactionTrigger = 1'000'000;  // never compact: keep many overlapping tables
        kv::lsm::LsmTree db(o);
        std::mt19937_64 rng(2);
        for (std::uint64_t i = 0; i < keys; ++i) db.put(zeroPadded((rng() % keys) * 2), value);  // even keys only
        db.flush();

        const int lookups = g_quick ? 5000 : 20000;
        double present = 0, absent = 0;
        for (int pass = 0; pass < 2; ++pass) {
            auto start = Clock::now();
            std::size_t found = 0;
            for (int i = 0; i < lookups; ++i)
                found += db.get(zeroPadded((rng() % keys) * 2 + (pass == 0 ? 0 : 1))).has_value();
            (pass == 0 ? present : absent) = lookups / secondsSince(start);
            (void)found;
        }
        std::printf("| %s (%zu tables) | %.0f | %.0f |\n", bits ? "10 bits/key" : "no filter",
                    db.stats().tablesPerLevel[0], present, absent);
    }

    std::printf("\n### Range scan\n\n");
    {
        Scratch dir;
        kv::lsm::LsmTree db(lsmOptions(dir.file("db"), 4 << 20, 10));
        for (std::uint64_t i = 0; i < n; ++i) db.put(zeroPadded(i), value);
        db.flush();
        auto start = Clock::now();
        std::size_t seen = 0;
        db.forEach([&](const std::string&, const std::string&) { ++seen; });
        std::printf("Scanning all %zu entries in key order: %.0f entries/s\n", seen, seen / secondsSince(start));
    }
}

// ---------------------------------------------------------------------------
// Cluster
// ---------------------------------------------------------------------------

// Three servers in this process, each with its own thread, directory and TCP port.
class BenchCluster {
public:
    struct Options {
        bool fsync = true;
        bool groupCommit = true;
        bool readIndex = true;
        kv::server::ReplicatedNode::Engine engine = kv::server::ReplicatedNode::Engine::Memory;
    };

    explicit BenchCluster(Options o) {
        const std::size_t size = 3;
        std::map<kv::raft::NodeId, kv::net::Address> peers;
        for (kv::raft::NodeId id = 1; id <= size; ++id) {
            kv::server::NodeServer::Options so;
            so.id = id;
            so.dataDir = dir_.file("node" + std::to_string(id));
            so.tickInterval = 20ms;
            so.syncOnAppend = o.fsync;
            so.groupCommit = o.groupCommit;
            so.readIndex = o.readIndex;
            so.engine = o.engine;
            servers_.push_back(std::make_unique<kv::server::NodeServer>(so));
            peers[id] = {"127.0.0.1", servers_.back()->port()};
        }
        for (auto& s : servers_) {
            s->start(peers);
            threads_.emplace_back([&s] { s->run(); });
        }
        for (const auto& [id, a] : peers) addresses_.push_back(a.toString());

        // Wait for a leader: the first request that succeeds.
        kv::client::Client c(clientOptions());
        for (int i = 0; i < 100 && !c.put("warmup", "x").ok; ++i) {}
    }

    ~BenchCluster() {
        for (auto& s : servers_) s->stop();
        for (auto& t : threads_) t.join();
    }

    kv::client::Client::Options clientOptions() const {
        kv::client::Client::Options o;
        o.servers = addresses_;
        o.totalTimeout = 20s;
        return o;
    }

private:
    Scratch dir_;
    std::vector<std::unique_ptr<kv::server::NodeServer>> servers_;
    std::vector<std::thread> threads_;
    std::vector<std::string> addresses_;
};

struct LoadResult {
    double opsPerSec = 0;
    Latencies latencies;
    std::uint64_t failures = 0;
};

// `threads` clients run `op` in a loop for `seconds`, measuring each call.
template <class Op>
LoadResult runLoad(const BenchCluster& cluster, int threads, double seconds, Op op) {
    std::atomic<bool> stop{false};
    std::vector<Latencies> perThread(static_cast<std::size_t>(threads));
    std::atomic<std::uint64_t> ops{0}, failures{0};
    std::vector<std::thread> workers;
    auto start = Clock::now();
    for (int t = 0; t < threads; ++t)
        workers.emplace_back([&, t] {
            kv::client::Client client(cluster.clientOptions());
            std::mt19937_64 rng(static_cast<std::uint64_t>(t) + 1);
            while (!stop) {
                auto begin = Clock::now();
                const bool ok = op(client, rng);
                perThread[static_cast<std::size_t>(t)].ms.push_back(
                    std::chrono::duration<double, std::milli>(Clock::now() - begin).count());
                ++ops;
                if (!ok) ++failures;
            }
        });
    std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    stop = true;
    for (auto& w : workers) w.join();

    LoadResult r;
    r.opsPerSec = static_cast<double>(ops) / secondsSince(start);
    for (auto& l : perThread) r.latencies.merge(l);
    r.failures = failures;
    return r;
}

void printRow(const std::string& label, int threads, LoadResult& r) {
    std::printf("| %s | %d | %.0f | %.2f | %.2f | %.2f |%s\n", label.c_str(), threads, r.opsPerSec,
                r.latencies.percentile(50), r.latencies.percentile(99), r.latencies.percentile(99.9),
                r.failures ? " (some operations failed)" : "");
}

void benchCluster() {
    std::printf("\n## Cluster: 3 servers over loopback TCP, every write replicated to a majority\n\n");
    const double seconds = g_quick ? 2 : 4;
    const std::string value(100, 'v');
    auto putOp = [&](kv::client::Client& c, std::mt19937_64& rng) { return c.put("key" + std::to_string(rng() % 100000), value).ok; };

    std::printf("### Writes: group commit\n\n");
    std::printf("Each write is appended to every node's log and fsync'ed before it is acknowledged. With group commit a node\n");
    std::printf("handles every request that arrived in the same event-loop iteration with ONE fsync.\n\n");
    std::printf("| configuration | clients | ops/s | p50 ms | p99 ms | p99.9 ms |\n|---|---|---|---|---|---|\n");
    for (bool group : {false, true}) {
        BenchCluster::Options o;
        o.groupCommit = group;
        BenchCluster cluster(o);
        for (int clients : {1, 8, 32}) {
            auto r = runLoad(cluster, clients, seconds, putOp);
            printRow(group ? "fsync, group commit" : "fsync, one per request", clients, r);
        }
    }
    {
        BenchCluster::Options o;
        o.fsync = false;
        BenchCluster cluster(o);
        for (int clients : {1, 32}) {
            auto r = runLoad(cluster, clients, seconds, putOp);
            printRow("no fsync (RAM speed)", clients, r);
        }
    }

    std::printf("\n### Reads: ReadIndex against reads through the log\n\n");
    std::printf("| configuration | clients | ops/s | p50 ms | p99 ms | p99.9 ms |\n|---|---|---|---|---|---|\n");
    for (bool readIndex : {false, true}) {
        BenchCluster::Options o;
        o.readIndex = readIndex;
        BenchCluster cluster(o);
        {
            kv::client::Client c(cluster.clientOptions());
            for (int i = 0; i < 1000; ++i) c.put("key" + std::to_string(i), value);
        }
        auto getOp = [&](kv::client::Client& c, std::mt19937_64& rng) { return c.get("key" + std::to_string(rng() % 1000)).ok; };
        for (int clients : {1, 32}) {
            auto r = runLoad(cluster, clients, seconds, getOp);
            printRow(readIndex ? "ReadIndex (no log entry)" : "read through the log", clients, r);
        }
    }

    std::printf("\n### Storage engine: memory against LSM tree\n\n");
    std::printf("| engine | clients | ops/s | p50 ms | p99 ms | p99.9 ms |\n|---|---|---|---|---|---|\n");
    for (auto engine : {kv::server::ReplicatedNode::Engine::Memory, kv::server::ReplicatedNode::Engine::Lsm}) {
        BenchCluster::Options o;
        o.engine = engine;
        BenchCluster cluster(o);
        auto r = runLoad(cluster, 32, seconds, putOp);
        printRow(engine == kv::server::ReplicatedNode::Engine::Memory ? "memory" : "LSM tree", 32, r);
    }
}

}  // namespace

int main(int argc, char** argv) {
    std::string what = "all";
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--quick") g_quick = true;
        else if (arg == "--dir" && i + 1 < argc) g_root = argv[++i];
        else what = arg;
    }
    fs::create_directories(g_root);
    std::printf("# kvbench%s\n", g_quick ? " (quick)" : "");
    std::printf("\nOne machine, loopback network, files in %s. Compare rows with each other, not with other machines.\n",
                g_root.c_str());
    if (onTmpfs(g_root))
        std::printf("\n**Warning:** %s is on tmpfs (RAM), where fsync is free. Pass --dir with a path on a real disk.\n",
                    g_root.c_str());
    if (what == "all" || what == "wal") benchWal();
    if (what == "all" || what == "lsm") benchLsm();
    if (what == "all" || what == "cluster") benchCluster();
    return 0;
}
