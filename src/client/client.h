#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "net/frame_decoder.h"
#include "net/packet.h"
#include "raft/types.h"
#include "net/socket.h"
#include "storage/file_util.h"

namespace kv::client {

// A blocking client for the replicated store.
//
// It's given the addresses of the servers and does the awkward parts of
// talking to a cluster:
//   - Finding the leader. Any node accepts a connection, but only the
//     leader takes requests. A follower answers "not leader" and, if it
//     knows, who is, and the client follows that hint.
//   - Surviving failures. A dead server, a refused connection or a timeout
//     just means "try the next server".
//   - Retrying safely. Every operation gets a sequence number, and a retry
//     reuses it. The cluster remembers the last answer for each client, so
//     an operation whose answer was lost is not executed a second time.
//
// Not thread-safe: use one Client per thread.
class Client {
public:
    struct Options {
        std::vector<std::string> servers;  // "host:port" of the cluster's nodes
        std::chrono::milliseconds attemptTimeout{1000};  // how long to wait on one server
        std::chrono::milliseconds totalTimeout{15000};   // give up after this long overall
        std::uint64_t clientId = 0;  // 0: pick a random one
    };

    struct Result {
        bool ok = false;
        bool found = false;  // Get/Del: did the key exist?
        std::string value;   // Get: the value
        // If !ok. A timeout means the outcome is UNKNOWN: the operation may or
        // may not have been applied.
        std::string error;
    };

    // Throws std::invalid_argument if no server address is valid.
    explicit Client(Options options);

    Result put(const std::string& key, const std::string& value);
    Result get(const std::string& key);
    Result del(const std::string& key);

    // Cluster administration: add or remove ONE server. The new server must
    // already be running (started with --join). Ok once the change has
    // committed. A rejection means it was invalid (already a member, not a
    // member, ...) or another change is still in progress: wait and retry.
    Result addServer(raft::NodeId id, const std::string& address);
    Result removeServer(raft::NodeId id);

    std::uint64_t clientId() const { return clientId_; }

private:
    Result execute(const storage::LogRecord& op);

    // Sends `request` (which carries sequence number `seq`) until the cluster
    // gives a final answer, following leader hints and retrying with the same
    // seq. Used for every kind of request.
    Result run(const net::Packet& request, std::uint64_t seq);

    // One attempt against the current server. Returns true and fills
    // `response` if the server answered the request with this seq.
    bool attempt(const net::Packet& request, std::uint64_t seq, net::ClientResponse& response,
                 std::chrono::steady_clock::time_point deadline);
    bool ensureConnected(std::chrono::steady_clock::time_point deadline);
    void disconnect();
    void useNextServer();
    void useServer(const net::Address& address);

    Options options_;
    std::vector<net::Address> servers_;
    std::size_t current_ = 0;
    std::uint64_t clientId_;
    std::uint64_t nextSeq_ = 1;

    storage::UniqueFd fd_;
    net::FrameDecoder decoder_;
};

}  // namespace kv::client
