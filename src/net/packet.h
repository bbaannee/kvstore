#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "raft/message.h"
#include "storage/log_record.h"

namespace kv::net {

// A client asks the cluster to run one operation.
//
// (clientId, seq) identify the request. A client that gets no answer retries
// with the SAME seq, and the state machine runs it only once (see
// KvStateMachine). Use a new seq for each new operation.
struct ClientRequest {
    std::uint64_t clientId = 0;
    std::uint64_t seq = 0;
    storage::LogRecord op;  // Put, Del or Get

    bool operator==(const ClientRequest&) const = default;
};

enum class ClientStatus : std::uint8_t {
    Ok = 0,         // the operation was committed and applied
    NotLeader = 1,  // ask someone else; leaderAddr says who, if known
    Aborted = 2,    // our entry was overwritten, so it did NOT happen; retry
    TooLarge = 3,   // the key or value exceeds the limits in net/limits.h; retrying won't help
    Rejected = 4,   // refused as invalid right now (see ClientResponse::message); nothing happened
};

struct ClientResponse {
    std::uint64_t seq = 0;  // which request this answers
    ClientStatus status = ClientStatus::Ok;
    bool found = false;     // Get/Del: did the key exist?
    std::string value;      // Get: the value
    std::string leaderAddr; // NotLeader: "host:port" of the leader, or empty

    bool operator==(const ClientResponse&) const = default;
};

// A server introduces itself on every outgoing connection it opens: "I am
// node `id`, and you can reach me at `address`". A server that was just added
// to the cluster has no other way to learn where the leader is, and it needs to
// answer the leader.
struct Hello {
    raft::NodeId id = 0;
    std::string address;  // "host:port"

    bool operator==(const Hello&) const = default;
};

// An administrator asks the cluster to add or remove ONE server. The answer is
// a ClientResponse with the same seq (Ok once the change has committed;
// Rejected if it is invalid or another change is still pending).
struct MembershipRequest {
    std::uint64_t seq = 0;
    bool add = true;           // false: remove
    raft::NodeId nodeId = 0;
    std::string address;       // for add: where the new server listens

    bool operator==(const MembershipRequest&) const = default;
};

// Everything that travels over a connection. Peers send raft::Message (and
// Hello) to each other; clients send ClientRequest and MembershipRequest and
// receive ClientResponse.
using Packet = std::variant<raft::Message, ClientRequest, ClientResponse, Hello, MembershipRequest>;

// Packet layout (integers little-endian):
//   [type: u8]  1 = raft message, 2 = client request, 3 = client response, 4 = hello, 5 = membership request
//   raft message:    [from u64][to u64][term u64][kind u8] + body
//   client request:  [client_id u64][seq u64][LogRecord payload to the end]
//   client response: [seq u64][status u8][found u8][value: u32 len + bytes][leader_addr: u32 len + bytes]
//   hello:           [id u64][address: u32 len + bytes]
//   membership req.: [seq u64][add u8][node_id u64][address: u32 len + bytes]
std::string encodePacket(const Packet& packet);

// Parses a packet received from the network. Returns false if the bytes are
// malformed. Never reads out of bounds and never trusts a length.
bool decodePacket(std::string_view bytes, Packet& out);

}  // namespace kv::net
