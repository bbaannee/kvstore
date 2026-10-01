#include "cli/command.h"

#include <gtest/gtest.h>

using kv::cli::Command;
using kv::cli::parseCommand;

TEST(Command, ValueIsRestOfLine) {
    Command c = parseCommand("put msg hello world");
    EXPECT_EQ(c.type, Command::Type::Put);
    EXPECT_EQ(c.key, "msg");
    EXPECT_EQ(c.value, "hello world");
}

TEST(Command, IsCaseInsensitive) {
    EXPECT_EQ(parseCommand("GET k").type, Command::Type::Get);
    EXPECT_EQ(parseCommand("Del k").type, Command::Type::Del);
    EXPECT_EQ(parseCommand("exit").type, Command::Type::Exit);
}

TEST(Command, RejectsMissingArguments) {
    EXPECT_EQ(parseCommand("").type, Command::Type::Unknown);
    EXPECT_EQ(parseCommand("put k").type, Command::Type::Unknown);
    EXPECT_EQ(parseCommand("get").type, Command::Type::Unknown);
    EXPECT_EQ(parseCommand("frobnicate x").type, Command::Type::Unknown);
}
