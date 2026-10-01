#include "storage/log_writer.h"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>

#include "storage/log_format.h"

namespace kv::storage {

LogWriter::LogWriter(const std::string& path) {
    bool existed = std::filesystem::exists(path);
    fd_ = openOrThrow(path, O_WRONLY | O_CREAT | O_APPEND);
    if (!existed) fsyncParentDir(path);
}

void LogWriter::append(const LogRecord& rec) {
    writeAll(frameRecord(encodePayload(rec)));
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
