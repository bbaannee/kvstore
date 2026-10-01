cmake_minimum_required(VERSION 3.20)
project(kvstore LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)  # lets VS Code find includes

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

# cmake -DKV_SANITIZE=ON: catch memory bugs and undefined behavior at runtime.
option(KV_SANITIZE "Build with AddressSanitizer + UndefinedBehaviorSanitizer" OFF)
if(KV_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

# Small building blocks shared by everything: byte encoding, checksums.
add_library(kv_common
  src/common/crc32c.cpp
)
target_include_directories(kv_common PUBLIC src)

# Storage engine: write-ahead log + in-memory map. Knows nothing about the CLI.
add_library(kv_storage
  src/storage/file_util.cpp
  src/storage/log_format.cpp
  src/storage/log_record.cpp
  src/storage/log_reader.cpp
  src/storage/log_writer.cpp
  src/storage/kv_store.cpp
)
target_link_libraries(kv_storage PUBLIC kv_common)

# Raft consensus as a pure state machine: no network, disk or clock access.
add_library(kv_raft
  src/raft/raft_log.cpp
  src/raft/raft_node.cpp
)
target_include_directories(kv_raft PUBLIC src)

# Command parsing + REPL, kept in a library so tests can use them.
add_library(kv_cli
  src/cli/command.cpp
  src/cli/repl.cpp
)
target_link_libraries(kv_cli PUBLIC kv_storage)

add_executable(kv src/cli/main.cpp)
target_link_libraries(kv PRIVATE kv_cli)

# Tests
enable_testing()
find_package(GTest REQUIRED)
add_executable(kv_tests
  tests/coding_test.cpp
  tests/crc32c_test.cpp
  tests/log_record_test.cpp
  tests/log_reader_test.cpp
  tests/kv_store_test.cpp
  tests/command_test.cpp
  tests/repl_test.cpp
  tests/raft/raft_node_test.cpp
  tests/raft/raft_cluster_test.cpp
)
target_include_directories(kv_tests PRIVATE tests)
target_link_libraries(kv_tests PRIVATE kv_cli kv_raft GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(kv_tests)
