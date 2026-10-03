#include "net/packet.h"

#include "common/coding.h"
#include "raft/entry_codec.h"
#include "raft/members_codec.h"

namespace kv::net {

namespace {

enum class PacketType : std::uint8_t { Raft = 1, ClientRequest = 2, ClientResponse = 3, Hello = 4, MembershipRequest = 5 };

constexpr std::size_t kMaxAddressSize = 256;

// Values and addresses can't be bigger than this on the wire.
constexpr std::size_t kMaxStringSize = 64 * 1024 * 1024;

void putBool(std::string& out, bool v) { out += static_cast<char>(v ? 1 : 0); }

bool getBool(ByteReader& in, bool& v) {
    std::uint8_t b;
    if (!in.getU8(b) || b > 1) return false;
    v = b == 1;
    return true;
}

void putString(std::string& out, const std::string& s) {
    putFixed32(out, static_cast<std::uint32_t>(s.size()));
    out += s;
}

// --- raft message bodies ---------------------------------------------------
// The kind byte is the index of the alternative in raft::Message::body.

void encodeBody(std::string& out, const raft::RequestVote& b) {
    putFixed64(out, b.lastLogIndex);
    putFixed64(out, b.lastLogTerm);
}
void encodeBody(std::string& out, const raft::RequestVoteResponse& b) { putBool(out, b.granted); }
void encodeBody(std::string& out, const raft::AppendEntries& b) {
    putFixed64(out, b.prevLogIndex);
    putFixed64(out, b.prevLogTerm);
    putFixed64(out, b.leaderCommit);
    putFixed64(out, b.seq);
    putFixed32(out, static_cast<std::uint32_t>(b.entries.size()));
    for (const raft::Entry& e : b.entries) raft::putEntry(out, e);
}
void encodeBody(std::string& out, const raft::AppendEntriesResponse& b) {
    putBool(out, b.success);
    putFixed64(out, b.matchIndex);
    putFixed64(out, b.rejectHint);
    putFixed64(out, b.seq);
}
void encodeBody(std::string& out, const raft::InstallSnapshot& b) {
    putFixed64(out, b.lastIndex);
    putFixed64(out, b.lastTerm);
    raft::putMembers(out, b.members);
    putFixed64(out, b.offset);
    putString(out, b.data);
    putBool(out, b.done);
}
void encodeBody(std::string& out, const raft::InstallSnapshotResponse& b) {
    putFixed64(out, b.lastIndex);
    putFixed64(out, b.nextOffset);
    putBool(out, b.done);
}

bool decodeRaft(ByteReader& in, raft::Message& m) {
    std::uint8_t kind;
    if (!in.getFixed64(m.from) || !in.getFixed64(m.to) || !in.getFixed64(m.term) || !in.getU8(kind)) return false;

    switch (kind) {
        case 0: {
            raft::RequestVote b;
            if (!in.getFixed64(b.lastLogIndex) || !in.getFixed64(b.lastLogTerm)) return false;
            m.body = b;
            return true;
        }
        case 1: {
            raft::RequestVoteResponse b;
            if (!getBool(in, b.granted)) return false;
            m.body = b;
            return true;
        }
        case 2: {
            raft::AppendEntries b;
            std::uint32_t count;
            if (!in.getFixed64(b.prevLogIndex) || !in.getFixed64(b.prevLogTerm) || !in.getFixed64(b.leaderCommit) ||
                !in.getFixed64(b.seq) || !in.getFixed32(count) ||
                count > in.remaining() / raft::kMinEncodedEntrySize)  // no huge reserve()
                return false;
            b.entries.reserve(count);
            for (std::uint32_t i = 0; i < count; ++i) {
                raft::Entry e;
                if (!raft::getEntry(in, e)) return false;
                b.entries.push_back(std::move(e));
            }
            m.body = std::move(b);
            return true;
        }
        case 3: {
            raft::AppendEntriesResponse b;
            if (!getBool(in, b.success) || !in.getFixed64(b.matchIndex) || !in.getFixed64(b.rejectHint) ||
                !in.getFixed64(b.seq))
                return false;
            m.body = b;
            return true;
        }
        case 4: {
            raft::InstallSnapshot b;
            if (!in.getFixed64(b.lastIndex) || !in.getFixed64(b.lastTerm) || !raft::getMembers(in, b.members) ||
                !in.getFixed64(b.offset) || !in.getLengthPrefixed(kMaxStringSize, b.data) || !getBool(in, b.done))
                return false;
            m.body = std::move(b);
            return true;
        }
        case 5: {
            raft::InstallSnapshotResponse b;
            if (!in.getFixed64(b.lastIndex) || !in.getFixed64(b.nextOffset) || !getBool(in, b.done)) return false;
            m.body = b;
            return true;
        }
        default:
            return false;
    }
}

}  // namespace

std::string encodePacket(const Packet& packet) {
    std::string out;
    if (const auto* m = std::get_if<raft::Message>(&packet)) {
        out += static_cast<char>(PacketType::Raft);
        putFixed64(out, m->from);
        putFixed64(out, m->to);
        putFixed64(out, m->term);
        out += static_cast<char>(m->body.index());
        std::visit([&](const auto& body) { encodeBody(out, body); }, m->body);
    } else if (const auto* req = std::get_if<ClientRequest>(&packet)) {
        out += static_cast<char>(PacketType::ClientRequest);
        putFixed64(out, req->clientId);
        putFixed64(out, req->seq);
        out += storage::encodePayload(req->op);
    } else if (const auto* resp = std::get_if<ClientResponse>(&packet)) {
        out += static_cast<char>(PacketType::ClientResponse);
        putFixed64(out, resp->seq);
        out += static_cast<char>(resp->status);
        putBool(out, resp->found);
        putString(out, resp->value);
        putString(out, resp->leaderAddr);
    } else if (const auto* hello = std::get_if<Hello>(&packet)) {
        out += static_cast<char>(PacketType::Hello);
        putFixed64(out, hello->id);
        putString(out, hello->address);
    } else {
        const auto& req = std::get<MembershipRequest>(packet);
        out += static_cast<char>(PacketType::MembershipRequest);
        putFixed64(out, req.seq);
        putBool(out, req.add);
        putFixed64(out, req.nodeId);
        putString(out, req.address);
    }
    return out;
}

bool decodePacket(std::string_view bytes, Packet& out) {
    ByteReader in(bytes);
    std::uint8_t type;
    if (!in.getU8(type)) return false;

    switch (static_cast<PacketType>(type)) {
        case PacketType::Raft: {
            raft::Message m;
            if (!decodeRaft(in, m) || !in.atEnd()) return false;
            out = std::move(m);
            return true;
        }
        case PacketType::ClientRequest: {
            ClientRequest req;
            std::string payload;
            if (!in.getFixed64(req.clientId) || !in.getFixed64(req.seq) || !in.getBytes(in.remaining(), payload))
                return false;
            if (!storage::decodePayload(payload, req.op)) return false;
            out = std::move(req);
            return true;
        }
        case PacketType::ClientResponse: {
            ClientResponse resp;
            std::uint8_t status;
            if (!in.getFixed64(resp.seq) || !in.getU8(status) || status > static_cast<std::uint8_t>(ClientStatus::Rejected) ||
                !getBool(in, resp.found) || !in.getLengthPrefixed(kMaxStringSize, resp.value) ||
                !in.getLengthPrefixed(kMaxStringSize, resp.leaderAddr) || !in.atEnd())
                return false;
            resp.status = static_cast<ClientStatus>(status);
            out = std::move(resp);
            return true;
        }
        case PacketType::Hello: {
            Hello hello;
            if (!in.getFixed64(hello.id) || !in.getLengthPrefixed(kMaxAddressSize, hello.address) || !in.atEnd())
                return false;
            out = std::move(hello);
            return true;
        }
        case PacketType::MembershipRequest: {
            MembershipRequest req;
            if (!in.getFixed64(req.seq) || !getBool(in, req.add) || !in.getFixed64(req.nodeId) ||
                !in.getLengthPrefixed(kMaxAddressSize, req.address) || !in.atEnd())
                return false;
            out = std::move(req);
            return true;
        }
    }
    return false;
}

}  // namespace kv::net
