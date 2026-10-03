#include "raft/members_codec.h"

#include <algorithm>
#include <cstdint>

namespace kv::raft {

void putMembers(std::string& out, const Members& members) {
    putFixed32(out, static_cast<std::uint32_t>(members.size()));
    for (const Member& m : members) {
        putFixed64(out, m.id);
        putFixed32(out, static_cast<std::uint32_t>(m.address.size()));
        out += m.address;
    }
}

bool getMembers(ByteReader& in, Members& members) {
    std::uint32_t count;
    if (!in.getFixed32(count) || count > kMaxMembers) return false;
    members.clear();
    members.reserve(count);
    for (std::uint32_t i = 0; i < count; ++i) {
        Member m;
        if (!in.getFixed64(m.id) || !in.getLengthPrefixed(kMaxAddressSize, m.address)) return false;
        if (!members.empty() && members.back().id >= m.id) return false;  // must be strictly increasing
        members.push_back(std::move(m));
    }
    return true;
}

std::string encodeMembers(const Members& members) {
    std::string out;
    putMembers(out, members);
    return out;
}

bool decodeMembers(std::string_view bytes, Members& members) {
    ByteReader in(bytes);
    return getMembers(in, members) && in.atEnd();
}

Members sortedMembers(Members members) {
    std::sort(members.begin(), members.end(), [](const Member& a, const Member& b) { return a.id < b.id; });
    return members;
}

}  // namespace kv::raft
