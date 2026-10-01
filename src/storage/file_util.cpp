#include "storage/file_util.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <system_error>
#include <utility>

namespace kv::storage {

UniqueFd::~UniqueFd() {
    if (fd_ >= 0) ::close(fd_);
}

UniqueFd::UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

UniqueFd& UniqueFd::operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
        if (fd_ >= 0) ::close(fd_);
        fd_ = std::exchange(other.fd_, -1);
    }
    return *this;
}

void throwErrno(const std::string& what) {
    throw std::system_error(errno, std::generic_category(), what);
}

UniqueFd openOrThrow(const std::string& path, int flags, int mode) {
    int fd = ::open(path.c_str(), flags, mode);
    if (fd < 0) throwErrno("open " + path);
    return UniqueFd(fd);
}

void fsyncFile(const std::string& path) {
    UniqueFd fd = openOrThrow(path, O_RDONLY);
    if (::fsync(fd.get()) != 0) throwErrno("fsync " + path);
}

void fsyncParentDir(const std::string& path) {
    std::string dir = std::filesystem::absolute(path).parent_path().string();
    UniqueFd fd = openOrThrow(dir, O_RDONLY | O_DIRECTORY);
    if (::fsync(fd.get()) != 0) throwErrno("fsync dir " + dir);
}

void truncateFile(const std::string& path, std::uint64_t size) {
    if (::truncate(path.c_str(), static_cast<off_t>(size)) != 0) throwErrno("truncate " + path);
    fsyncFile(path);
}

}  // namespace kv::storage
