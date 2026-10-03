// kvctl: command-line client for the replicated key-value store.
//
//   kvctl --servers 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 put name Bane
//   kvctl --servers ... get name
//   kvctl --servers ... del name
//
//   kvctl --servers ... add-server 4 127.0.0.1:7004     (the server must already be running with --join)
//   kvctl --servers ... remove-server 3
//
// The value of `put` is everything after the key. Exit status: 0 on
// success, 1 if a get/del found no such key, 2 on errors.

#include <cstdlib>
#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include "client/client.h"

namespace {

void usage() {
    std::cerr << "Usage: kvctl --servers HOST:PORT,HOST:PORT,... [--timeout-ms N] put KEY VALUE...\n"
              << "       kvctl --servers ... get KEY\n"
              << "       kvctl --servers ... del KEY\n"
              << "       kvctl --servers ... add-server ID HOST:PORT | remove-server ID\n";
}

}  // namespace

int main(int argc, char** argv) {
    kv::client::Client::Options options;
    std::vector<std::string> words;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--servers" && i + 1 < argc) {
            std::string list = argv[++i];
            std::size_t start = 0;
            while (start <= list.size()) {
                std::size_t comma = list.find(',', start);
                options.servers.push_back(list.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
                if (comma == std::string::npos) break;
                start = comma + 1;
            }
        } else if (arg == "--timeout-ms" && i + 1 < argc) {
            options.totalTimeout = std::chrono::milliseconds(std::stoi(argv[++i]));
        } else {
            words.push_back(arg);
        }
    }

    if (options.servers.empty() || words.empty()) {
        usage();
        return 2;
    }
    const std::string& command = words[0];

    try {
        kv::client::Client client(options);

        if (command == "put" && words.size() >= 3) {
            std::string value = words[2];
            for (std::size_t i = 3; i < words.size(); ++i) value += " " + words[i];
            auto r = client.put(words[1], value);
            if (!r.ok) {
                std::cerr << "error: " << r.error << "\n";
                return 2;
            }
            std::cout << "OK\n";
        } else if (command == "add-server" && words.size() == 3) {
            auto r = client.addServer(std::stoull(words[1]), words[2]);
            if (!r.ok) {
                std::cerr << "error: " << r.error << "\n";
                return 2;
            }
            std::cout << "OK\n";
        } else if (command == "remove-server" && words.size() == 2) {
            auto r = client.removeServer(std::stoull(words[1]));
            if (!r.ok) {
                std::cerr << "error: " << r.error << "\n";
                return 2;
            }
            std::cout << "OK\n";
        } else if (command == "get" && words.size() == 2) {
            auto r = client.get(words[1]);
            if (!r.ok) {
                std::cerr << "error: " << r.error << "\n";
                return 2;
            }
            if (!r.found) {
                std::cout << "(not found)\n";
                return 1;
            }
            std::cout << r.value << "\n";
        } else if (command == "del" && words.size() == 2) {
            auto r = client.del(words[1]);
            if (!r.ok) {
                std::cerr << "error: " << r.error << "\n";
                return 2;
            }
            std::cout << (r.found ? "OK" : "(not found)") << "\n";
            return r.found ? 0 : 1;
        } else {
            usage();
            return 2;
        }
    } catch (const std::exception& e) {
        std::cerr << "kvctl: " << e.what() << "\n";
        return 2;
    }
    return 0;
}
