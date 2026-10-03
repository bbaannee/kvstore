#pragma once

#include <string>
#include <string_view>

#include "storage/file_util.h"
#include "storage/log_record.h"

namespace kv::storage {

// Appends records to the end of a log file and makes each one durable.
//
// Why not std::ofstream + flush()? flush() only moves data from our buffer
// into the OS page cache (RAM). That survives kill -9, but not a power cut.
// fsync() makes the OS write the data to disk and waits until it's done.
//
// The log doesn't care what a payload means: KvStore logs LogRecords, and
// RaftStorage logs Raft state updates. Both get the same framing, checksums
// and durability.
class LogWriter {
public:
    // Opens the log for appending, creating it if needed. Pass
    // syncOnAppend=false only in tests that simulate process crashes (which
    // keep the OS page cache) and don't need the speed hit of a real fsync.
    explicit LogWriter(const std::string& path, bool syncOnAppend = true);

    // Returns only once the record is safely on disk (if syncOnAppend).
    void append(const LogRecord& rec) { appendPayload(encodePayload(rec)); }
    void appendPayload(std::string_view payload);

    // Forces everything appended so far to disk. Needed only when
    // syncOnAppend is false: the owner then decides when to pay for an fsync.
    void sync();

private:
    void writeAll(const std::string& bytes);

    UniqueFd fd_;
    bool syncOnAppend_;
};

}  // namespace kv::storage
