#include "cli/command.h"

#include <cctype>
#include <sstream>

namespace kv::cli {

Command parseCommand(const std::string& line) {
    std::istringstream iss(line);
    std::string op;
    Command cmd;
    iss >> op >> cmd.key;
    std::getline(iss >> std::ws, cmd.value);  // rest of the line
    for (char& c : op) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));

    using T = Command::Type;
    if (op == "PUT" && !cmd.key.empty() && !cmd.value.empty()) cmd.type = T::Put;
    else if (op == "GET" && !cmd.key.empty()) cmd.type = T::Get;
    else if (op == "DEL" && !cmd.key.empty()) cmd.type = T::Del;
    else if (op == "EXIT") cmd.type = T::Exit;
    return cmd;
}

}  // namespace kv::cli
