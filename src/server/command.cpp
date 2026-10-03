#include "server/command.h"

#include "common/coding.h"

namespace kv::server {

std::string encodeCommand(const Command& command) {
    std::string out;
    putFixed64(out, command.clientId);
    putFixed64(out, command.seq);
    out += storage::encodePayload(command.op);
    return out;
}

bool decodeCommand(std::string_view bytes, Command& out) {
    ByteReader in(bytes);
    std::string op;
    if (!in.getFixed64(out.clientId) || !in.getFixed64(out.seq) || !in.getBytes(in.remaining(), op)) return false;
    return storage::decodePayload(op, out.op);
}

}  // namespace kv::server
