#pragma once

#include <istream>
#include <ostream>

#include "cli/command.h"
#include "storage/kv_store.h"

namespace kv::cli {

// Interactive prompt: reads commands from `in`, runs them against the
// store, and prints results to `out`. Takes streams instead of using
// std::cin/std::cout directly so tests can drive it with strings.
class Repl {
public:
    Repl(storage::KvStore& store, std::istream& in, std::ostream& out)
        : store_(store), in_(in), out_(out) {}

    // Runs until "exit" or end of input (Ctrl+D).
    void run();

private:
    // Returns false when the REPL should stop.
    bool execute(const Command& cmd);

    storage::KvStore& store_;
    std::istream& in_;
    std::ostream& out_;
};

}  // namespace kv::cli
