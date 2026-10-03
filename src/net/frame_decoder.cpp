#include "net/frame_decoder.h"

#include "common/coding.h"
#include "common/crc32c.h"
#include "storage/log_format.h"

namespace kv::net {

FrameDecoder::Status FrameDecoder::next(std::string& payload) {
    if (!error_.empty()) return Status::Error;

    const std::size_t available = buffer_.size() - pos_;
    if (available < storage::kHeaderSize) return Status::NeedMore;

    const char* header = buffer_.data() + pos_;
    const std::uint32_t length = decodeFixed32(header);
    const std::uint32_t lengthCrc = decodeFixed32(header + 4);
    const std::uint32_t payloadCrc = decodeFixed32(header + 8);

    // Check the length BEFORE waiting for that many bytes: a damaged length
    // must not make us wait forever or buffer gigabytes.
    if (crc32c(std::string_view(header, 4)) != lengthCrc) return fail("bad length checksum");
    if (length > maxPayload_) return fail("frame too large");
    if (available - storage::kHeaderSize < length) return Status::NeedMore;

    std::string_view body(header + storage::kHeaderSize, length);
    if (crc32c(body) != payloadCrc) return fail("bad payload checksum");

    payload.assign(body);
    pos_ += storage::kHeaderSize + length;

    // Drop what we've consumed, but not on every frame (that would be a copy per frame).
    if (pos_ == buffer_.size()) {
        buffer_.clear();
        pos_ = 0;
    } else if (pos_ >= 64 * 1024 && pos_ * 2 >= buffer_.size()) {
        buffer_.erase(0, pos_);
        pos_ = 0;
    }
    return Status::Frame;
}

FrameDecoder::Status FrameDecoder::fail(std::string why) {
    error_ = std::move(why);
    buffer_.clear();
    pos_ = 0;
    return Status::Error;
}

}  // namespace kv::net
