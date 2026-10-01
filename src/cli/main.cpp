// kv: interactive command-line front end for the key-value store.
//
// Usage: kv [log_file]   (default: data.log in the current directory)

#include <exception>
#include <iostream>
#include <string>

#include "cli/repl.h"
#include "storage/kv_store.h"

int main(int argc, char** argv) {
    const std::string logPath = argc > 1 ? argv[1] : "data.log";
    try {
        kv::storage::KvStore store(logPath);

        const auto& rec = store.recovery();
        if (rec.truncatedTornTail)
            std::cerr << "Warning: incomplete record at the end of " << logPath
                      << ", truncated log to " << rec.validBytes << " bytes\n";
        std::cout << "Loaded " << rec.recordsReplayed << " records from " << logPath << "\n";

        kv::cli::Repl(store, std::cin, std::cout).run();
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
