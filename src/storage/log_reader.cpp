#include "storage/log_reader.h"

#include <algorithm>
#include <filesystem>
#include <system_error>

#include "common/coding.h"
#include "common/crc32c.h"
#include "storage/log_format.h"

namespace kv::storage {

LogReader::LogReader(const std::string& path) : in_(path, std::ios::binary) {
    std::error_code ec;
    auto size = std::filesystem::file_size(path, ec);
    fileSize_ = ec ? 0 : size;  // missing file = empty log
}

// The recovery rules. "Torn" is what a crash mid-write can leave behind, and
// it's always at the end. Anything else is corruption.
ReadStatus LogReader::next(LogRecord& out) {
    if (pos_ == fileSize_) return ReadStatus::End;

    // 1. Not even a full header left: the header was being written.
    if (fileSize_ - pos_ < kHeaderSize) return ReadStatus::Torn;

    const std::string header = readAt(pos_, kHeaderSize);
    const std::uint32_t length = decodeFixed32(header.data());
    const std::uint32_t lengthCrc = decodeFixed32(header.data() + 4);
    const std::uint32_t payloadCrc = decodeFixed32(header.data() + 8);

    // 2. Damaged length. Zeros all the way to the end are a crash artifact
    //    (the file grew but the data never arrived); anything else isn't.
    if (crc32c(header.substr(0, 4)) != lengthCrc) {
        if (onlyZerosFrom(pos_)) return ReadStatus::Torn;
        return corrupt("bad length checksum");
    }

    // 3. A verified length can't be this big unless the writer was broken.
    if (length > kMaxPayloadSize) return corrupt("record length too large");

    // 4. The length is trustworthy, so a payload running past the end of
    //    the file really is incomplete.
    const std::uint64_t end = pos_ + kHeaderSize + length;
    if (end > fileSize_) return ReadStatus::Torn;

    // 5. Damaged payload: fine if it's the last record (or only zeros
    //    follow it), corruption otherwise.
    const std::string payload = readAt(pos_ + kHeaderSize, length);
    if (crc32c(payload) != payloadCrc) {
        if (end == fileSize_ || onlyZerosFrom(end)) return ReadStatus::Torn;
        return corrupt("bad payload checksum");
    }

    // Checksum OK but unparseable: written by a buggy writer.
    if (!decodePayload(payload, out)) return corrupt("malformed record");

    pos_ = end;
    return ReadStatus::Ok;
}

ReadStatus LogReader::corrupt(const std::string& what) {
    error_ = what + " in record at byte " + std::to_string(pos_);
    return ReadStatus::Corrupt;
}

std::string LogReader::readAt(std::uint64_t offset, std::size_t n) {
    std::string buf(n, '\0');
    in_.clear();
    in_.seekg(static_cast<std::streamoff>(offset));
    in_.read(buf.data(), static_cast<std::streamsize>(n));
    if (static_cast<std::size_t>(in_.gcount()) != n)
        throw std::runtime_error("failed to read log at byte " + std::to_string(offset));
    return buf;
}

bool LogReader::onlyZerosFrom(std::uint64_t offset) {
    constexpr std::size_t kChunk = 64 * 1024;
    while (offset < fileSize_) {
        const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(kChunk, fileSize_ - offset));
        const std::string chunk = readAt(offset, n);
        if (std::any_of(chunk.begin(), chunk.end(), [](char c) { return c != '\0'; })) return false;
        offset += n;
    }
    return true;
}

}  // namespace kv::storage
