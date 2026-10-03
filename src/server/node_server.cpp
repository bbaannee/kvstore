#include "server/node_server.h"

#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <random>
#include <set>

#include "net/limits.h"
#include "raft/members_codec.h"
#include "storage/log_format.h"

namespace kv::server {

namespace {

using Clock = std::chrono::steady_clock;

// A slow or dead peer must not make us buffer without limit. Past this, we
// drop messages (Raft retries) or, for a client, give up on the connection.
constexpr std::size_t kMaxPendingBytes = 8 * 1024 * 1024;

constexpr std::chrono::milliseconds kReconnectDelay{100};

}  // namespace

NodeServer::NodeServer(Options options) : options_(std::move(options)) {
    listenFd_ = net::listenTcp(options_.listen, port_);

    epollFd_ = storage::UniqueFd(::epoll_create1(EPOLL_CLOEXEC));
    if (!epollFd_.valid()) storage::throwErrno("epoll_create1");
    stopFd_ = storage::UniqueFd(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (!stopFd_.valid()) storage::throwErrno("eventfd");

    for (int fd : {listenFd_.get(), stopFd_.get()}) {
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (::epoll_ctl(epollFd_.get(), EPOLL_CTL_ADD, fd, &ev) != 0) storage::throwErrno("epoll_ctl");
    }
}

NodeServer::~NodeServer() = default;

std::string NodeServer::advertisedAddress() const {
    return net::Address{options_.listen.host, port_}.toString();
}

void NodeServer::start(const std::map<raft::NodeId, net::Address>& peers) {
    // A server that was started as a joiner stays one across restarts. If it
    // had to be told on every start, a joiner restarted before it ever got
    // any log would write a bootstrap configuration of its own, and diverge.
    std::filesystem::create_directories(options_.dataDir);
    const std::filesystem::path marker = std::filesystem::path(options_.dataDir) / "joining";
    if (options_.join) std::ofstream{marker};
    const bool joining = options_.join || std::filesystem::exists(marker);

    raft::Config cfg;
    cfg.id = options_.id;
    cfg.joining = joining;
    cfg.bootstrapConfigEntry = !joining;
    for (const auto& [id, address] : peers) {
        cfg.peers.push_back(id);
        cfg.addresses[id] = address.toString();
    }
    // Different seeds make nodes pick different election timeouts, which is
    // what lets one of them win an election before the others even start.
    std::random_device rd;
    cfg.randomSeed = (static_cast<std::uint64_t>(rd()) << 32) ^ rd() ^ options_.id;

    ReplicatedNode::Options nodeOptions;
    nodeOptions.syncOnAppend = options_.syncOnAppend;
    nodeOptions.snapshotThreshold = options_.snapshotThreshold;
    nodeOptions.engine = options_.engine;
    nodeOptions.lsm = options_.lsm;
    node_ = std::make_unique<ReplicatedNode>(options_.dataDir, std::move(cfg), nodeOptions);
    node_->setBatching(options_.groupCommit);

    peerAddresses_ = peers;
    syncMembership();
}

void NodeServer::stop() {
    // write() is async-signal-safe, so this works from a signal handler too.
    std::uint64_t one = 1;
    [[maybe_unused]] ssize_t n = ::write(stopFd_.get(), &one, sizeof(one));
}

void NodeServer::log(const std::string& message) const {
    if (options_.verbose) std::cerr << "[node " << options_.id << "] " << message << std::endl;
}

// ---------------------------------------------------------------------------
// The event loop
// ---------------------------------------------------------------------------

void NodeServer::run() {
    if (!node_) throw std::logic_error("NodeServer::run() called before start()");
    log("listening on " + advertisedAddress());
    nextTick_ = Clock::now() + options_.tickInterval;
    finishIteration();
    maintainPeers();  // start connecting to the other nodes right away

    constexpr int kMaxEvents = 64;
    epoll_event events[kMaxEvents];

    while (!stopping_) {
        // Sleep until a socket is ready or the next tick is due, whichever is first.
        auto untilTick = std::chrono::duration_cast<std::chrono::milliseconds>(nextTick_ - Clock::now());
        int timeoutMs = static_cast<int>(std::max<std::int64_t>(0, untilTick.count() + 1));

        int n = ::epoll_wait(epollFd_.get(), events, kMaxEvents, timeoutMs);
        if (n < 0) {
            if (errno == EINTR) continue;
            storage::throwErrno("epoll_wait");
        }

        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            if (fd == stopFd_.get()) {
                stopping_ = true;
            } else if (fd == listenFd_.get()) {
                acceptAll();
            } else if (auto it = conns_.find(fd); it != conns_.end() && !it->second->dead) {
                onEvent(*it->second, events[i].events);
            }
        }
        reapDead();
        runTicks();
        finishIteration();
        reapDead();
    }
    log("stopped");
}

void NodeServer::runTicks() {
    auto now = Clock::now();
    int caughtUp = 0;
    while (now >= nextTick_) {
        node_->tick();
        nextTick_ += options_.tickInterval;
        // After a long stall (a slow fsync, a suspended process), don't fire
        // a burst of ticks that would trigger a pointless election.
        if (++caughtUp >= 3) {
            nextTick_ = now + options_.tickInterval;
            break;
        }
    }
    if (caughtUp > 0) maintainPeers();
}

// Everything the handlers and ticks asked the node to do is done now, in the
// required order: persist (one fsync for the whole batch), then answer clients
// and send messages.
void NodeServer::finishIteration() {
    node_->flush();
    logStateChanges();
    pump();
    syncMembership();
}

void NodeServer::acceptAll() {
    while (true) {
        int fd = ::accept4(listenFd_.get(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR) continue;
            return;  // EAGAIN: no more waiting connections (or a transient error; we'll be called again)
        }
        net::setNoDelay(fd);
        if (options_.sendBufferBytes > 0) net::setSendBufferSize(fd, options_.sendBufferBytes);
        Conn& c = addConn(storage::UniqueFd(fd), /*connecting=*/false);
        log("accepted connection " + std::to_string(c.id));
    }
}

void NodeServer::onEvent(Conn& c, std::uint32_t events) {
    if (c.connecting) {
        if (events & (EPOLLOUT | EPOLLERR | EPOLLHUP)) finishConnect(c);
        return;
    }
    if (events & EPOLLERR) {
        closeConn(c, "socket error");
        return;
    }
    // Read before reacting to a hangup: the peer may have sent data and then closed.
    if (events & (EPOLLIN | EPOLLHUP | EPOLLRDHUP)) onReadable(c);
    if (!c.dead && (events & EPOLLOUT)) flushConn(c);
}

void NodeServer::onReadable(Conn& c) {
    char buf[64 * 1024];
    while (!c.dead) {
        ssize_t n = ::recv(c.fd.get(), buf, sizeof(buf), 0);
        if (n == 0) {
            closeConn(c, "closed by peer");
            return;
        }
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            closeConn(c, std::string("recv: ") + std::strerror(errno));
            return;
        }

        // Our outgoing connections are send-only; ignore anything that arrives on them.
        if (c.peer) continue;

        c.decoder.feed(std::string_view(buf, static_cast<std::size_t>(n)));
        std::string payload;
        while (!c.dead) {
            auto status = c.decoder.next(payload);
            if (status == net::FrameDecoder::Status::NeedMore) break;
            if (status == net::FrameDecoder::Status::Error) {
                closeConn(c, "protocol error: " + c.decoder.error());
                return;
            }
            handleFrame(c, payload);
        }
    }
}

// ---------------------------------------------------------------------------
// Incoming messages
// ---------------------------------------------------------------------------

void NodeServer::handleFrame(Conn& c, const std::string& payload) {
    net::Packet packet;
    if (!net::decodePacket(payload, packet)) {
        closeConn(c, "malformed packet");
        return;
    }
    if (auto* m = std::get_if<raft::Message>(&packet)) handleRaftMessage(c, *m);
    else if (auto* req = std::get_if<net::ClientRequest>(&packet)) handleClientRequest(c, *req);
    else if (auto* mem = std::get_if<net::MembershipRequest>(&packet)) handleMembershipRequest(c, *mem);
    else if (auto* hello = std::get_if<net::Hello>(&packet)) handleHello(*hello);
    else closeConn(c, "unexpected packet type");  // a server never receives responses
}

void NodeServer::handleRaftMessage(Conn& c, const raft::Message& m) {
    // Only trust messages addressed to us. We don't check the sender against
    // the configuration: a server that was just added hears from a leader it
    // has never heard of, and who may talk to whom is Raft's business.
    if (m.to != options_.id || m.from == options_.id) {
        closeConn(c, "raft message with a bad sender or receiver");
        return;
    }
    node_->step(m);  // the output is sent at the end of the iteration (finishIteration)
}

void NodeServer::handleHello(const net::Hello& hello) {
    if (hello.id == options_.id) return;
    if (auto address = net::parseAddress(hello.address)) {
        peerAddresses_[hello.id] = *address;
        log("node " + std::to_string(hello.id) + " says it is at " + hello.address);
    }
}

void NodeServer::handleClientRequest(Conn& c, const net::ClientRequest& req) {
    if (req.op.key.size() > net::kMaxKeySize || req.op.value.size() > net::kMaxValueSize) {
        // Refuse before it goes anywhere near the log.
        queue(c, net::Packet(net::ClientResponse{req.seq, net::ClientStatus::TooLarge, false, {}, {}}));
        return;
    }

    // The node answers once the request is decided, which may be much later
    // (or immediately with NotLeader), and by then the connection may be gone.
    // That's why the callback holds the connection's id, not a reference.
    const std::uint64_t connId = c.id;
    const std::uint64_t seq = req.seq;
    auto answer = [this, connId, seq](RequestStatus status, const ApplyResult& result) {
        respond(connId, seq, status, result);
    };

    if (req.op.type == storage::LogRecord::Type::Get && options_.readIndex) {
        // A linearizable read without a log entry. Reads change nothing, so a
        // retry needs no deduplication.
        node_->read(req.op.key, answer);
    } else {
        node_->submit(Command{req.clientId, req.seq, req.op}, answer);
    }
}

void NodeServer::handleMembershipRequest(Conn& c, const net::MembershipRequest& req) {
    const std::uint64_t connId = c.id;
    const std::uint64_t seq = req.seq;
    auto answer = [this, connId, seq](RequestStatus status, const ApplyResult& result) {
        respond(connId, seq, status, result);
    };

    // The change is the current configuration plus or minus ONE server. (Only the
    // leader's view is authoritative; a follower is told to ask the leader.)
    raft::Members members = node_->raft().members();
    auto existing = std::find_if(members.begin(), members.end(), [&](const raft::Member& m) { return m.id == req.nodeId; });
    if (req.add) {
        if (existing != members.end() || !net::parseAddress(req.address)) {
            answer(RequestStatus::Rejected, {});  // already a member, or no usable address
            return;
        }
        members.push_back(raft::Member{req.nodeId, req.address});
    } else {
        if (existing == members.end()) {
            answer(RequestStatus::Rejected, {});  // not a member
            return;
        }
        members.erase(existing);
    }
    node_->changeMembership(raft::sortedMembers(std::move(members)), answer);
}

// ---------------------------------------------------------------------------
// Outgoing messages
// ---------------------------------------------------------------------------

void NodeServer::logStateChanges() {
    const raft::RaftNode& r = node_->raft();
    if (r.configIndex() != loggedConfig_) {
        loggedConfig_ = r.configIndex();
        std::string ids;
        for (const auto& m : r.members()) ids += (ids.empty() ? "" : ",") + std::to_string(m.id);
        log("configuration at index " + std::to_string(loggedConfig_) + ": members " + ids);
    }
    if (r.role() == loggedRole_ && r.term() == loggedTerm_) return;
    loggedRole_ = r.role();
    loggedTerm_ = r.term();
    const char* role = loggedRole_ == raft::Role::Leader ? "LEADER" : loggedRole_ == raft::Role::Candidate ? "candidate" : "follower";
    log(std::string("term ") + std::to_string(loggedTerm_) + ": now " + role + ", commit index " + std::to_string(r.commitIndex()));
}

void NodeServer::pump() {
    for (raft::Message& m : node_->takeOutgoing()) {
        Conn* c = nullptr;
        if (auto it = peerFd_.find(m.to); it != peerFd_.end()) {
            c = conns_.at(it->second).get();
        } else {
            // Not connected. Anybody we have a message for and an address for gets a
            // connection: we must be able to answer whoever talks to us, including
            // a server that was just added and isn't in our configuration yet.
            auto retry = retryAt_.find(m.to);
            if (retry == retryAt_.end() || Clock::now() >= retry->second) c = connectPeer(m.to);
        }
        if (!c || c->dead) continue;  // can't reach it right now: drop the message, Raft will retry
        queue(*c, net::Packet(std::move(m)));
    }
}

void NodeServer::respond(std::uint64_t connId, std::uint64_t seq, RequestStatus status, const ApplyResult& result) {
    auto it = connById_.find(connId);
    if (it == connById_.end()) return;  // the client hung up; nothing to tell it

    net::ClientResponse resp;
    resp.seq = seq;
    resp.found = result.found;
    resp.value = result.value;
    switch (status) {
        case RequestStatus::Committed:
            resp.status = net::ClientStatus::Ok;
            break;
        case RequestStatus::NotLeader: {
            resp.status = net::ClientStatus::NotLeader;
            // Tell the client who the leader is, if we know, so it doesn't have to guess.
            auto leader = node_->raft().leader();
            if (leader) {
                auto addr = peerAddresses_.find(*leader);
                if (addr != peerAddresses_.end()) resp.leaderAddr = addr->second.toString();
            }
            break;
        }
        case RequestStatus::Aborted:
            resp.status = net::ClientStatus::Aborted;
            break;
        case RequestStatus::Rejected:
            resp.status = net::ClientStatus::Rejected;
            break;
    }
    queue(*conns_.at(it->second), net::Packet(std::move(resp)));
}

bool NodeServer::queue(Conn& c, const net::Packet& packet) {
    if (c.dead) return false;
    if (c.out.size() - c.outPos > kMaxPendingBytes) {
        if (!c.peer) closeConn(c, "client is not reading its answers");
        return false;  // a slow peer: drop the message
    }
    c.out += storage::frameRecord(net::encodePacket(packet));
    c.lastUsed = Clock::now();
    if (!c.connecting) flushConn(c);  // while connecting, the bytes wait until the connection is up
    return true;
}

void NodeServer::flushConn(Conn& c) {
    while (!c.dead && c.outPos < c.out.size()) {
        ssize_t n = ::send(c.fd.get(), c.out.data() + c.outPos, c.out.size() - c.outPos, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  // socket buffer full: wait for EPOLLOUT
            closeConn(c, std::string("send: ") + std::strerror(errno));
            return;
        }
        c.outPos += static_cast<std::size_t>(n);
    }
    if (c.dead) return;
    if (c.outPos == c.out.size()) {
        c.out.clear();
        c.outPos = 0;
    }
    c.wantWrite = !c.out.empty();
    updateInterest(c);
}

void NodeServer::updateInterest(Conn& c) {
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (c.wantWrite || c.connecting) ev.events |= EPOLLOUT;
    ev.data.fd = c.fd.get();
    ::epoll_ctl(epollFd_.get(), EPOLL_CTL_MOD, c.fd.get(), &ev);
}

// ---------------------------------------------------------------------------
// Connections
// ---------------------------------------------------------------------------

NodeServer::Conn& NodeServer::addConn(storage::UniqueFd fd, bool connecting) {
    auto conn = std::make_unique<Conn>();
    conn->id = nextConnId_++;
    conn->connecting = connecting;
    const int rawFd = fd.get();
    conn->fd = std::move(fd);

    Conn& ref = *conn;
    conns_[rawFd] = std::move(conn);
    connById_[ref.id] = rawFd;

    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLRDHUP;
    if (connecting) ev.events |= EPOLLOUT;
    ev.data.fd = rawFd;
    ::epoll_ctl(epollFd_.get(), EPOLL_CTL_ADD, rawFd, &ev);
    return ref;
}

// Marks the connection dead right away but frees it only at the end of the
// loop iteration (reapDead), because the caller, or code further up the
// stack, may still be holding a reference to it.
void NodeServer::closeConn(Conn& c, const std::string& why) {
    if (c.dead) return;
    c.dead = true;
    log("closing connection " + std::to_string(c.id) + ": " + why);
    ::epoll_ctl(epollFd_.get(), EPOLL_CTL_DEL, c.fd.get(), nullptr);
    connById_.erase(c.id);  // answers for this client are no longer deliverable
    if (c.peer) {
        peerFd_.erase(*c.peer);
        retryAt_[*c.peer] = Clock::now() + kReconnectDelay;
    }
}

void NodeServer::reapDead() {
    for (auto it = conns_.begin(); it != conns_.end();) {
        if (it->second->dead) it = conns_.erase(it);
        else ++it;
    }
}

// Remembers the address of every member the configuration (in the Raft log)
// tells us about. This is how a server learns about servers added after it started.
void NodeServer::syncMembership() {
    for (const raft::Member& m : node_->raft().members()) {
        if (m.id == options_.id || m.address.empty()) continue;
        if (auto address = net::parseAddress(m.address)) peerAddresses_[m.id] = *address;
    }
}

// We keep an outgoing connection to every member of the configuration, and
// to the current leader (a server that was just added doesn't know the
// configuration yet, but it must be able to answer the leader).
void NodeServer::maintainPeers() {
    std::set<raft::NodeId> wanted;
    for (const raft::Member& m : node_->raft().members())
        if (m.id != options_.id) wanted.insert(m.id);
    if (auto leader = node_->raft().leader(); leader && *leader != options_.id) wanted.insert(*leader);

    // A connection to a server that is not in the configuration (it was removed, or
    // we only answered it once) is closed when it has been unused for a while.
    const auto now = Clock::now();
    for (auto it = peerFd_.begin(); it != peerFd_.end();) {
        auto next = std::next(it);
        Conn& c = *conns_.at(it->second);
        if (!wanted.count(it->first) && now - c.lastUsed > std::chrono::seconds(5)) closeConn(c, "no longer needed");
        it = next;
    }

    for (raft::NodeId peer : wanted) {
        if (peerFd_.count(peer)) continue;
        auto retry = retryAt_.find(peer);
        if (retry != retryAt_.end() && now < retry->second) continue;
        connectPeer(peer);
    }
}

NodeServer::Conn* NodeServer::connectPeer(raft::NodeId peer) {
    auto addr = peerAddresses_.find(peer);
    if (addr == peerAddresses_.end()) return nullptr;
    try {
        bool inProgress = false;
        storage::UniqueFd fd = net::startConnect(addr->second, inProgress);
        if (options_.sendBufferBytes > 0) net::setSendBufferSize(fd.get(), options_.sendBufferBytes);
        Conn& c = addConn(std::move(fd), inProgress);
        c.peer = peer;
        c.lastUsed = Clock::now();
        peerFd_[peer] = c.fd.get();
        sendHello(c);  // always the first thing on the connection (it waits there while we are still connecting)
        if (!inProgress) log("connected to node " + std::to_string(peer));
        return &c;
    } catch (const std::exception& e) {
        log("cannot connect to node " + std::to_string(peer) + ": " + e.what());
        retryAt_[peer] = Clock::now() + kReconnectDelay;
        return nullptr;
    }
}

void NodeServer::sendHello(Conn& c) {
    queue(c, net::Packet(net::Hello{options_.id, advertisedAddress()}));
}

void NodeServer::finishConnect(Conn& c) {
    if (int err = net::connectError(c.fd.get()); err != 0) {
        closeConn(c, std::string("connect failed: ") + std::strerror(err));
        return;
    }
    c.connecting = false;
    log("connected to node " + std::to_string(*c.peer));
    flushConn(c);  // send what was queued while we were connecting (starting with the Hello)
}

}  // namespace kv::server
