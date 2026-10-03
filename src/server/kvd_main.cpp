// kvd: one node of the replicated key-value store.
//
// A 3-node cluster on one machine, one command per terminal:
//   kvd --id 1 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data /tmp/kv1
//   kvd --id 2 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data /tmp/kv2
//   kvd --id 3 --peers 1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 --data /tmp/kv3
//
// --peers lists EVERY node of the initial cluster, including this one; this
// node listens on the address listed for --id.
//
// Adding a 4th server to the running cluster:
//   kvd --id 4 --listen 127.0.0.1:7004 --join --data /tmp/kv4        (start it, then...)
//   kvctl --servers 127.0.0.1:7001 add-server 4 127.0.0.1:7004       (...tell the cluster)

#include <csignal>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <map>
#include <string>
#include <string_view>

#include "server/node_server.h"

namespace {

kv::server::NodeServer* g_server = nullptr;

void onSignal(int) {
    if (g_server) g_server->stop();  // stop() is async-signal-safe
}

void usage(const char* program) {
    std::cerr << "Usage: " << program << " --id N --data DIR (--peers ID=HOST:PORT,ID=HOST:PORT,... | --listen HOST:PORT --join)\n"
              << "          [--tick-ms 50] [--snapshot-every N] [--engine memory|lsm] [--memtable-mb N]\n"
              << "          [--no-group-commit] [--no-read-index]\n"
              << "          [--no-fsync] [--quiet]\n";
}

}  // namespace

int main(int argc, char** argv) {
    kv::server::NodeServer::Options options;
    options.verbose = true;
    std::map<kv::raft::NodeId, kv::net::Address> peers;
    std::optional<kv::net::Address> listen;

    for (int i = 1; i < argc; ++i) {
        std::string_view arg = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) {
                usage(argv[0]);
                std::exit(2);
            }
            return argv[++i];
        };

        if (arg == "--id") {
            options.id = std::stoull(value());
        } else if (arg == "--data") {
            options.dataDir = value();
        } else if (arg == "--listen") {
            listen = kv::net::parseAddress(value());
            if (!listen) {
                std::cerr << "bad --listen address (expected HOST:PORT)\n";
                return 2;
            }
        } else if (arg == "--join") {
            options.join = true;
        } else if (arg == "--tick-ms") {
            options.tickInterval = std::chrono::milliseconds(std::stoi(value()));
        } else if (arg == "--snapshot-every") {
            options.snapshotThreshold = std::stoull(value());
        } else if (arg == "--engine") {
            std::string engine = value();
            if (engine == "memory") options.engine = kv::server::ReplicatedNode::Engine::Memory;
            else if (engine == "lsm") options.engine = kv::server::ReplicatedNode::Engine::Lsm;
            else {
                std::cerr << "--engine must be 'memory' or 'lsm'\n";
                return 2;
            }
        } else if (arg == "--memtable-mb") {
            options.lsm.memtableBytes = std::stoull(value()) * 1024 * 1024;
        } else if (arg == "--no-group-commit") {
            options.groupCommit = false;
        } else if (arg == "--no-read-index") {
            options.readIndex = false;
        } else if (arg == "--no-fsync") {
            options.syncOnAppend = false;  // unsafe: for experiments only
        } else if (arg == "--quiet") {
            options.verbose = false;
        } else if (arg == "--peers") {
            std::string list = value();
            std::size_t start = 0;
            while (start <= list.size()) {
                std::size_t comma = list.find(',', start);
                std::string item = list.substr(start, comma == std::string::npos ? std::string::npos : comma - start);
                auto eq = item.find('=');
                auto addr = eq == std::string::npos ? std::nullopt : kv::net::parseAddress(item.substr(eq + 1));
                if (!addr) {
                    std::cerr << "bad --peers entry: '" << item << "' (expected ID=HOST:PORT)\n";
                    return 2;
                }
                peers[std::stoull(item.substr(0, eq))] = *addr;
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        } else {
            usage(argv[0]);
            return 2;
        }
    }

    if (options.id == 0 || options.dataDir.empty()) {
        usage(argv[0]);
        return 2;
    }
    if (listen) options.listen = *listen;
    else if (peers.count(options.id)) options.listen = peers.at(options.id);
    else {
        usage(argv[0]);
        std::cerr << "give --peers (which must include --id) or --listen\n";
        return 2;
    }

    try {
        kv::server::NodeServer server(std::move(options));
        // A joining server doesn't belong to the initial cluster; it learns the rest from the leader.
        server.start(peers);

        g_server = &server;
        std::signal(SIGINT, onSignal);
        std::signal(SIGTERM, onSignal);
        std::signal(SIGPIPE, SIG_IGN);

        server.run();
        g_server = nullptr;
    } catch (const std::exception& e) {
        std::cerr << "kvd: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
