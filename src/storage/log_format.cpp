#include "storage/log_format.h"

#include <cstdint>

#include "common/coding.h"
#include "common/crc32c.h"

namespace kv::storage {

std::string frameRecord(std::string_view payload) {
    std::string lengthBytes;
    putFixed32(lengthBytes, static_cast<std::uint32_t>(payload.size()));

    std::string out;
    out.reserve(kHeaderSize + payload.size());
    out += lengthBytes;
    putFixed32(out, crc32c(lengthBytes));
    putFixed32(out, crc32c(payload));
    out += payload;
    return out;
}

}  // namespace kv::storage
