#pragma once

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace kv::test {

// A fresh, empty directory for one test, deleted when the test ends.
// Every instance gets its own directory, even within one test, so a test can
// run two independent clusters side by side.
class TempDir {
public:
    TempDir() {
        static std::atomic<int> counter{0};
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        path_ = std::filesystem::temp_directory_path() /
                ("kv_" + std::string(info->test_suite_name()) + "_" + info->name() + "_" +
                 std::to_string(::getpid()) + "_" + std::to_string(counter++));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }

    std::string file(const std::string& name) const { return (path_ / name).string(); }

private:
    std::filesystem::path path_;
};

}  // namespace kv::test
