#include "cli/repl.h"

#include <string>

namespace kv::cli {

void Repl::run() {
    std::string line;
    while (out_ << "> " << std::flush && std::getline(in_, line)) {
        if (!execute(parseCommand(line))) return;
    }
    out_ << '\n';  // end of input (Ctrl+D)
}

bool Repl::execute(const Command& cmd) {
    using T = Command::Type;
    switch (cmd.type) {
        case T::Put:
            store_.put(cmd.key, cmd.value);
            out_ << "OK\n";
            break;
        case T::Get: {
            auto value = store_.get(cmd.key);
            out_ << (value ? *value : "(not found)") << '\n';
            break;
        }
        case T::Del:
            out_ << (store_.del(cmd.key) ? "OK" : "(not found)") << '\n';
            break;
        case T::Exit:
            return false;
        case T::Unknown:
            out_ << "Commands: put <k> <v> | get <k> | del <k> | exit\n";
            break;
    }
    return true;
}

}  // namespace kv::cli
