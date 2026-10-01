#pragma once

#include <string>

#include "storage/file_util.h"
#include "storage/log_record.h"

namespace kv::storage {

// Appends records to the end of a log file and makes each one durable.
//
// Why not std::ofstream + flush()? flush() only moves data from our buffer
// into the OS page cache (RAM). That survives kill -9, but not a power cut.
// fsync() makes the OS write the data to disk and waits until it's done.
class LogWriter {
public:
    // Opens the log for appending, creating it if needed.
    explicit LogWriter(const std::string& path);

    // Returns only once the record is safely on disk.
    void append(const LogRecord& rec);

private:
    void writeAll(const std::string& bytes);

    UniqueFd fd_;
};

}  // namespace kv::storage
