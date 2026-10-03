#include "raft/entry_codec.h"

#include <cstdint>

namespace kv::raft {

void putEntry(std::string& out, const Entry& entry) {
    putFixed64(out, entry.term);
    putFixed64(out, entry.index);
    out += static_cast<char>(entry.type);
    putFixed32(out, static_cast<std::uint32_t>(entry.data.size()));
    out += entry.data;
}

bool getEntry(ByteReader& in, Entry& entry) {
    std::uint8_t type;
    if (!in.getFixed64(entry.term) || !in.getFixed64(entry.index) || !in.getU8(type)) return false;
    if (type > static_cast<std::uint8_t>(Entry::Type::Config)) return false;
    entry.type = static_cast<Entry::Type>(type);
    return in.getLengthPrefixed(kMaxEntryDataSize, entry.data);
}

}  // namespace kv::raft
