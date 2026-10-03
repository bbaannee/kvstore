#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

// Fixed-width integers in little-endian byte order (lowest byte first).
//
// We build the bytes with shifts instead of memcpy'ing the integer, so the
// on-disk/on-wire format is the same no matter which CPU wrote it.

inline void putFixed32(std::string& dst, std::uint32_t v) {
    const char bytes[4] = {
        static_cast<char>(v & 0xFF),
        static_cast<char>((v >> 8) & 0xFF),
        static_cast<char>((v >> 16) & 0xFF),
        static_cast<char>((v >> 24) & 0xFF),
    };
    dst.append(bytes, 4);
}

inline void putFixed64(std::string& dst, std::uint64_t v) {
    putFixed32(dst, static_cast<std::uint32_t>(v & 0xFFFFFFFF));
    putFixed32(dst, static_cast<std::uint32_t>(v >> 32));
}

// Reads 4 bytes starting at `p`. The caller guarantees they exist.
inline std::uint32_t decodeFixed32(const char* p) {
    const auto* b = reinterpret_cast<const unsigned char*>(p);
    return static_cast<std::uint32_t>(b[0]) |
           static_cast<std::uint32_t>(b[1]) << 8 |
           static_cast<std::uint32_t>(b[2]) << 16 |
           static_cast<std::uint32_t>(b[3]) << 24;
}

inline std::uint64_t decodeFixed64(const char* p) {
    return static_cast<std::uint64_t>(decodeFixed32(p)) |
           static_cast<std::uint64_t>(decodeFixed32(p + 4)) << 32;
}

// Sequential, bounds-checked reader over a byte buffer.
//
// Every getter returns false if there aren't enough bytes left, so parsing
// untrusted data (a file that may be damaged, a message from the network)
// can never read past the end. After a failure, stop using the reader.
class ByteReader {
public:
    explicit ByteReader(std::string_view data) : data_(data) {}

    bool getU8(std::uint8_t& out) {
        if (remaining() < 1) return false;
        out = static_cast<std::uint8_t>(data_[pos_++]);
        return true;
    }

    bool getFixed32(std::uint32_t& out) {
        if (remaining() < 4) return false;
        out = decodeFixed32(data_.data() + pos_);
        pos_ += 4;
        return true;
    }

    bool getFixed64(std::uint64_t& out) {
        if (remaining() < 8) return false;
        out = decodeFixed64(data_.data() + pos_);
        pos_ += 8;
        return true;
    }

    // Reads exactly `n` bytes.
    bool getBytes(std::size_t n, std::string& out) {
        if (remaining() < n) return false;
        out.assign(data_.substr(pos_, n));
        pos_ += n;
        return true;
    }

    // Reads a u32 length followed by that many bytes, rejecting lengths
    // above `maxLen` before allocating anything.
    bool getLengthPrefixed(std::size_t maxLen, std::string& out) {
        std::uint32_t len;
        return getFixed32(len) && len <= maxLen && getBytes(len, out);
    }

    std::size_t remaining() const { return data_.size() - pos_; }
    bool atEnd() const { return remaining() == 0; }

private:
    std::string_view data_;
    std::size_t pos_ = 0;
};

}  // namespace kv
