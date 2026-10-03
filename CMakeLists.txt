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

# cmake -DKV_TSAN=ON: catch data races between threads (can't be combined with KV_SANITIZE).
option(KV_TSAN "Build with ThreadSanitizer" OFF)
if(KV_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
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

# The LSM-tree storage engine: memtable, SSTables, Bloom filters, leveled compaction.
add_library(kv_lsm
  src/lsm/bloom_filter.cpp
  src/lsm/memtable.cpp
  src/lsm/sstable.cpp
  src/lsm/cursor.cpp
  src/lsm/lsm_tree.cpp
)
target_link_libraries(kv_lsm PUBLIC kv_storage)

# Raft consensus as a pure state machine: no network, disk or clock access.
add_library(kv_raft
  src/raft/entry_codec.cpp
  src/raft/members_codec.cpp
  src/raft/raft_log.cpp
  src/raft/raft_node.cpp
)
target_link_libraries(kv_raft PUBLIC kv_common)

# Raft's term, vote and log on disk, in the same checksummed WAL as KvStore.
# Separate from kv_raft on purpose: the algorithm never depends on storage.
add_library(kv_raft_storage
  src/raft/raft_storage.cpp
)
target_link_libraries(kv_raft_storage PUBLIC kv_raft kv_storage)

# The replicated store: Raft + disk + key-value state machine.
add_library(kv_server
  src/server/command.cpp
  src/server/kv_storage.cpp
  src/server/kv_state_machine.cpp
  src/server/replicated_node.cpp
)
target_link_libraries(kv_server PUBLIC kv_raft_storage kv_lsm)

# Wire protocol: sockets, framing, and the binary encoding of every message.
add_library(kv_net
  src/net/frame_decoder.cpp
  src/net/packet.cpp
  src/net/socket.cpp
)
target_link_libraries(kv_net PUBLIC kv_raft kv_storage)

# One server process: the replicated node on real TCP sockets (epoll event loop).
add_library(kv_node_server
  src/server/node_server.cpp
)
target_link_libraries(kv_node_server PUBLIC kv_server kv_net)

# Client library: finds the leader, survives failures, retries safely.
add_library(kv_client
  src/client/client.cpp
)
target_link_libraries(kv_client PUBLIC kv_net)

add_executable(kvd src/server/kvd_main.cpp)
target_link_libraries(kvd PRIVATE kv_node_server)

add_executable(kvctl src/client/kvctl_main.cpp)
target_link_libraries(kvctl PRIVATE kv_client)

# Correctness checking: records client histories and decides linearizability.
add_library(kv_verify
  src/verify/history.cpp
  src/verify/linearizability.cpp
)
target_include_directories(kv_verify PUBLIC src)

# Command parsing + REPL, kept in a library so tests can use them.
add_library(kv_cli
  src/cli/command.cpp
  src/cli/repl.cpp
)
target_link_libraries(kv_cli PUBLIC kv_storage)

add_executable(kv src/cli/main.cpp)
target_link_libraries(kv PRIVATE kv_cli)

# Benchmarks (run with: ./build-release/kvbench). Build them with -DCMAKE_BUILD_TYPE=Release for meaningful numbers.
add_executable(kvbench bench/kvbench.cpp)
target_link_libraries(kvbench PRIVATE kv_node_server kv_client kv_lsm)

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
  tests/raft/raft_storage_test.cpp
  tests/raft/raft_storage_snapshot_test.cpp
  tests/raft/raft_node_snapshot_test.cpp
  tests/raft/raft_node_membership_test.cpp
  tests/raft/raft_node_leadership_test.cpp
  tests/raft/raft_cluster_snapshot_test.cpp
  tests/raft/raft_cluster_membership_test.cpp
  tests/raft/raft_log_test.cpp
  tests/raft/members_codec_test.cpp
  tests/server/kv_state_machine_test.cpp
  tests/server/kv_storage_engines_test.cpp
  tests/server/replicated_node_test.cpp
  tests/server/replicated_node_features_test.cpp
  tests/server/node_server_test.cpp
  tests/server/node_server_cluster_test.cpp
  tests/net/packet_test.cpp
  tests/net/frame_decoder_test.cpp
  tests/net/socket_test.cpp
  tests/lsm/lsm_components_test.cpp
  tests/lsm/lsm_tree_test.cpp
  tests/verify/linearizability_test.cpp
  tests/server/linearizability_cluster_test.cpp
)
find_package(Threads REQUIRED)
target_include_directories(kv_tests PRIVATE tests)
target_link_libraries(kv_tests PRIVATE kv_cli kv_node_server kv_client kv_verify kv_lsm Threads::Threads GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(kv_tests)
