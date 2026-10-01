#pragma once

#include <unistd.h>

#include <filesystem>
#include <string>

#include <gtest/gtest.h>

namespace kv::test {

// A fresh, empty directory for one test, deleted when the test ends.
class TempDir {
public:
    TempDir() {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        path_ = std::filesystem::temp_directory_path() /
                ("kv_" + std::string(info->test_suite_name()) + "_" + info->name() + "_" +
                 std::to_string(::getpid()));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }
    ~TempDir() { std::filesystem::remove_all(path_); }

    std::string file(const std::string& name) const { return (path_ / name).string(); }

private:
    std::filesystem::path path_;
};

}  // namespace kv::test
