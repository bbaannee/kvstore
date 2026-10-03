#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "net/frame_decoder.h"
#include "net/packet.h"
#include "net/socket.h"
#include "server/replicated_node.h"

namespace kv::server {

// One server process: a ReplicatedNode wired to real TCP sockets.
//
// Everything runs on ONE thread, in an epoll event loop:
//
//        ┌──────────── epoll_wait (sleeps until a socket is ready or a tick is due) ◀──┐
//        ▼                                                                             │
//   socket readable ─▶ FrameDecoder ─▶ decodePacket ─┬▶ raft message ─▶ node.step()     │
//                                                    ├▶ client request ─▶ node.submit() / node.read()
//                                                    ├▶ membership request ─▶ node.changeMembership()
//                                                    └▶ hello ─▶ remember where that peer is
//   tick due ─▶ node.tick()                                                            │
//        │                                                                             │
//   end of the iteration: node.flush() (ONE fsync for everything above), then send     │
//   what the node produced (raft messages to peers, answers to clients) ───────────────┘
//
// One thread means the (not thread-safe) node needs no locks and a message
// handler can never race with a tick. The only call that may come from
// another thread is stop().
//
// Group commit: with many requests arriving at once, the node would fsync once
// per request. Instead the node records what it wants to do while the loop
// handles every ready socket, and flush() persists it all with a single fsync
// before anything is sent or answered.
//
// Connections: each node opens ONE outgoing connection per peer and only
// sends on it; it receives on connections its peers opened to it. Clients
// connect like peers do and get their answers on the same connection.
// A message for a peer we're not connected to is dropped: Raft is built to
// tolerate lost messages (heartbeats retry), so no queueing is needed.
// Whom we connect to follows the cluster configuration in the Raft log, so
// adding or removing a server changes the connections by itself. Each
// outgoing connection starts with a Hello that says who we are and where we
// listen; that is how a server that was just added learns the leader's address.
class NodeServer {
public:
    struct Options {
        raft::NodeId id = 0;
        std::string dataDir;
        net::Address listen{"127.0.0.1", 0};         // port 0: let the OS pick (see port())
        std::chrono::milliseconds tickInterval{50};  // election timeout is 10-20 ticks, heartbeat 3

        // Start as a server that is about to be ADDED to a running cluster: it is
        // not a member yet, never starts elections, and learns the cluster from
        // the leader. Remembered in the data directory, so a restarted joiner
        // behaves the same way even if the flag is not repeated.
        bool join = false;

        std::size_t snapshotThreshold = 0;  // snapshot after this many applied entries (0: never)
        ReplicatedNode::Engine engine = ReplicatedNode::Engine::Memory;  // where the key-value data lives
        lsm::Options lsm;                   // tuning for the LSM engine (the directory is chosen by the node)
        bool readIndex = true;              // answer Gets with ReadIndex instead of through the log
        bool groupCommit = true;            // one fsync per event loop iteration instead of per request
        bool syncOnAppend = true;           // false is for tests only
        int sendBufferBytes = 0;            // tests only: shrink socket send buffers (0 = default)
        bool verbose = false;               // log connections, leadership and errors to stderr
    };

    // Binds the listening socket. Throws if that fails.
    explicit NodeServer(Options options);
    ~NodeServer();
    NodeServer(const NodeServer&) = delete;
    NodeServer& operator=(const NodeServer&) = delete;

    // The port we're listening on (useful when Options::listen.port was 0).
    std::uint16_t port() const { return port_; }

    // Opens the node's data directory and recovers from it. `peers` maps the
    // ids of the INITIAL cluster to their addresses, including this server;
    // a brand new cluster records it as its first configuration. A server that
    // is joining passes an empty map. Throws if the data is damaged.
    void start(const std::map<raft::NodeId, net::Address>& peers);

    // Runs the event loop until stop() is called.
    void run();

    // Makes run() return. Safe to call from any thread or a signal handler.
    void stop();

    // The node's state. Only safe to read while run() is not running.
    const ReplicatedNode& node() const { return *node_; }

private:
    struct Conn {
        std::uint64_t id = 0;
        storage::UniqueFd fd;
        net::FrameDecoder decoder;
        std::string out;          // bytes waiting to be sent
        std::size_t outPos = 0;   // how much of `out` was already sent
        bool connecting = false;  // outgoing connection not established yet
        bool wantWrite = false;   // registered for EPOLLOUT
        bool dead = false;        // closed; removed at the end of the loop iteration
        std::optional<raft::NodeId> peer;  // set on our outgoing connection to that peer
        std::chrono::steady_clock::time_point lastUsed;  // when we last queued something on it
    };

    // Event handling.
    void acceptAll();
    void onEvent(Conn& c, std::uint32_t events);
    void onReadable(Conn& c);
    void finishConnect(Conn& c);
    void handleFrame(Conn& c, const std::string& payload);
    void handleRaftMessage(Conn& c, const raft::Message& m);
    void handleClientRequest(Conn& c, const net::ClientRequest& req);
    void handleMembershipRequest(Conn& c, const net::MembershipRequest& req);
    void handleHello(const net::Hello& hello);

    // End of an event loop iteration: persist everything the node decided
    // (one fsync), answer clients, send messages, and follow the configuration.
    void finishIteration();
    void pump();  // send the messages the node queued for other nodes
    void respond(std::uint64_t connId, std::uint64_t seq, RequestStatus status, const ApplyResult& result);
    bool queue(Conn& c, const net::Packet& packet);
    void flushConn(Conn& c);
    void updateInterest(Conn& c);

    // Connection management.
    Conn& addConn(storage::UniqueFd fd, bool connecting);
    void closeConn(Conn& c, const std::string& why);
    void reapDead();
    void syncMembership();
    void maintainPeers();
    Conn* connectPeer(raft::NodeId peer);
    void sendHello(Conn& c);

    void runTicks();
    void logStateChanges();
    void log(const std::string& message) const;
    std::string advertisedAddress() const;

    Options options_;
    std::unique_ptr<ReplicatedNode> node_;  // created by start()
    std::map<raft::NodeId, net::Address> peerAddresses_;  // from the configuration and from Hellos

    storage::UniqueFd listenFd_;
    storage::UniqueFd epollFd_;
    storage::UniqueFd stopFd_;  // an eventfd: writing to it wakes the loop and stops it
    std::uint16_t port_ = 0;

    std::map<int, std::unique_ptr<Conn>> conns_;   // by file descriptor
    std::map<std::uint64_t, int> connById_;        // Conn::id -> fd, for answering clients
    std::map<raft::NodeId, int> peerFd_;           // our outgoing connection per peer
    std::map<raft::NodeId, std::chrono::steady_clock::time_point> retryAt_;
    raft::Role loggedRole_ = raft::Role::Follower;
    raft::Term loggedTerm_ = 0;
    raft::Index loggedConfig_ = 0;
    std::uint64_t nextConnId_ = 1;
    std::chrono::steady_clock::time_point nextTick_;
    bool stopping_ = false;
};

}  // namespace kv::server
