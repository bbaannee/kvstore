#pragma once

#include <cstdint>
#include <string>

namespace kv::storage {

// Owns a POSIX file descriptor and closes it automatically (RAII).
class UniqueFd {
public:
    UniqueFd() = default;
    explicit UniqueFd(int fd) : fd_(fd) {}
    ~UniqueFd();

    UniqueFd(UniqueFd&& other) noexcept;
    UniqueFd& operator=(UniqueFd&& other) noexcept;
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    int get() const { return fd_; }

private:
    int fd_ = -1;
};

// Throws std::system_error built from the current errno.
[[noreturn]] void throwErrno(const std::string& what);

// Opens a file with ::open() and throws on failure.
UniqueFd openOrThrow(const std::string& path, int flags, int mode = 0644);

// Forces a file's contents to disk.
void fsyncFile(const std::string& path);

// Forces the directory entry of `path` to disk. Needed after creating or
// renaming a file: until then, a power loss can make the file disappear.
void fsyncParentDir(const std::string& path);

// Cuts the file down to `size` bytes and makes that durable.
void truncateFile(const std::string& path, std::uint64_t size);

}  // namespace kv::storage
