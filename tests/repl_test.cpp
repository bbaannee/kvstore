#include "cli/repl.h"

#include <sstream>

#include <gtest/gtest.h>

#include "temp_dir.h"

TEST(Repl, RunsASession) {
    kv::test::TempDir dir;
    kv::storage::KvStore store(dir.file("data.log"));
    std::istringstream in("put msg hello world\nget msg\ndel msg\nget msg\nbogus\nexit\nget never\n");
    std::ostringstream out;

    kv::cli::Repl(store, in, out).run();

    EXPECT_EQ(out.str(),
              "> OK\n"
              "> hello world\n"
              "> OK\n"
              "> (not found)\n"
              "> Commands: put <k> <v> | get <k> | del <k> | exit\n"
              "> ");
}
