#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace kv::net {

// Cuts a TCP byte stream back into messages.
//
// TCP delivers bytes, not messages: one send() may arrive in several pieces,
// and several sends may arrive glued together. Feed the decoder whatever
// recv() returned, then call next() until it says NeedMore.
//
// Frames use the same layout as the write-ahead log (see storage/log_format.h):
//   [length u32][crc32c of length][crc32c of payload][payload]
//
// Unlike a log file, there is no "torn tail" on a connection, so any damage
// is a protocol error: the caller should close the connection.
class FrameDecoder {
public:
    enum class Status {
        Frame,     // a complete, verified payload was returned
        NeedMore,  // wait for more bytes
        Error,     // the stream is damaged; error() says how. Terminal.
    };

    static constexpr std::size_t kDefaultMaxPayload = 16 * 1024 * 1024;

    explicit FrameDecoder(std::size_t maxPayload = kDefaultMaxPayload) : maxPayload_(maxPayload) {}

    void feed(std::string_view bytes) { buffer_.append(bytes); }

    Status next(std::string& payload);

    const std::string& error() const { return error_; }

private:
    Status fail(std::string why);

    std::string buffer_;
    std::size_t pos_ = 0;  // start of the unread part of buffer_
    std::size_t maxPayload_;
    std::string error_;
};

}  // namespace kv::net
