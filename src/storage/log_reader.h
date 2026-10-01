#pragma once

#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>

#include "storage/log_record.h"

namespace kv::storage {

// Thrown when the log is damaged somewhere other than its end. We refuse to
// open such a log: guessing would risk silently losing or inventing data.
class CorruptLogError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

enum class ReadStatus {
    Ok,       // a record was read
    End,      // clean end of the log
    Torn,     // the log ends with a half-written record (a crash mid-write)
    Corrupt,  // a damaged record that a crash can't explain (see error())
};

// Reads records from a log file one by one, checking every checksum, and
// keeps track of where the last good record ends. A missing file reads as
// an empty log.
class LogReader {
public:
    explicit LogReader(const std::string& path);

    // Reads the next record into `out`. After anything other than Ok,
    // the reader stays stopped at that position.
    ReadStatus next(LogRecord& out);

    // Bytes from the start of the file to the end of the last good record.
    std::uint64_t validBytes() const { return pos_; }

    // What was wrong, after next() returned Corrupt.
    const std::string& error() const { return error_; }

private:
    ReadStatus corrupt(const std::string& what);

    // Reads exactly `n` bytes at `offset`; throws on I/O errors.
    std::string readAt(std::uint64_t offset, std::size_t n);

    // True if every byte from `offset` to the end of the file is zero.
    bool onlyZerosFrom(std::uint64_t offset);

    std::ifstream in_;
    std::uint64_t fileSize_ = 0;
    std::uint64_t pos_ = 0;  // start of the next record to read
    std::string error_;
};

}  // namespace kv::storage
