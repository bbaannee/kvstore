#pragma once

#include <string>

namespace kv::cli {

// A command typed by the user in the REPL.
struct Command {
    enum class Type { Put, Get, Del, Exit, Unknown };

    Type type = Type::Unknown;
    std::string key;
    std::string value;
};

// Parses a line like "put msg hello world".
// The command name is case-insensitive, the key is one word, and the value
// is the rest of the line (spaces included).
Command parseCommand(const std::string& line);

}  // namespace kv::cli
