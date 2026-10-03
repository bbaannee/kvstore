#include "storage/log_writer.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>

#include "storage/log_format.h"

namespace kv::storage {

LogWriter::LogWriter(const std::string& path, bool syncOnAppend) : syncOnAppend_(syncOnAppend) {
    bool existed = std::filesystem::exists(path);
    fd_ = openOrThrow(path, O_WRONLY | O_CREAT | O_APPEND);
    if (!existed && syncOnAppend_) fsyncParentDir(path);
}

void LogWriter::appendPayload(std::string_view payload) {
    writeAll(frameRecord(payload));
    if (syncOnAppend_ && ::fsync(fd_.get()) != 0) throwErrno("fsync log");
}

void LogWriter::sync() {
    if (::fsync(fd_.get()) != 0) throwErrno("fsync log");
}

void LogWriter::writeAll(const std::string& bytes) {
    const char* p = bytes.data();
    std::size_t left = bytes.size();
    while (left > 0) {  // write() may write fewer bytes than asked
        ssize_t n = ::write(fd_.get(), p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            throwErrno("write log");
        }
        p += n;
        left -= static_cast<std::size_t>(n);
    }
}

}  // namespace kv::storage
