#include "client/client.h"

#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cerrno>
#include <random>
#include <stdexcept>
#include <thread>

#include "net/limits.h"
#include "storage/log_format.h"

namespace kv::client {

namespace {

using Clock = std::chrono::steady_clock;

constexpr std::chrono::milliseconds kBackoff{30};
constexpr std::chrono::milliseconds kMaxBackoff{500};

// Waits until the socket is readable or the deadline passes.
bool waitReadable(int fd, Clock::time_point deadline) {
    while (true) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
        if (left.count() <= 0) return false;
        pollfd p{fd, POLLIN, 0};
        int rc = ::poll(&p, 1, static_cast<int>(left.count()));
        if (rc > 0) return true;
        if (rc == 0) return false;
        if (errno != EINTR) return false;
    }
}

bool sendAll(int fd, const std::string& bytes) {
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        ssize_t n = ::send(fd, bytes.data() + sent, bytes.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += static_cast<std::size_t>(n);
    }
    return true;
}

}  // namespace

Client::Client(Options options) : options_(std::move(options)) {
    for (const std::string& text : options_.servers)
        if (auto addr = net::parseAddress(text)) servers_.push_back(*addr);
    if (servers_.empty()) throw std::invalid_argument("client: no valid server address given");

    std::random_device rd;
    clientId_ = options_.clientId;
    while (clientId_ == 0) clientId_ = (static_cast<std::uint64_t>(rd()) << 32) ^ rd();

    // Start somewhere random so many clients don't all hit the same node first.
    current_ = rd() % servers_.size();
}

Client::Result Client::put(const std::string& key, const std::string& value) {
    return execute(storage::LogRecord::put(key, value));
}
Client::Result Client::get(const std::string& key) { return execute(storage::LogRecord::get(key)); }
Client::Result Client::del(const std::string& key) { return execute(storage::LogRecord::del(key)); }

Client::Result Client::execute(const storage::LogRecord& op) {
    if (op.key.size() > net::kMaxKeySize)
        return Result{false, false, {}, "key is " + std::to_string(op.key.size()) + " bytes; the maximum is " +
                                            std::to_string(net::kMaxKeySize)};
    if (op.value.size() > net::kMaxValueSize)
        return Result{false, false, {}, "value is " + std::to_string(op.value.size()) + " bytes; the maximum is " +
                                            std::to_string(net::kMaxValueSize)};

    // One seq for the whole operation, however many attempts it takes: that is
    // what lets the cluster recognize a retry and not run it twice.
    const std::uint64_t seq = nextSeq_++;
    return run(net::ClientRequest{clientId_, seq, op}, seq);
}

Client::Result Client::addServer(raft::NodeId id, const std::string& address) {
    const std::uint64_t seq = nextSeq_++;
    return run(net::MembershipRequest{seq, true, id, address}, seq);
}

Client::Result Client::removeServer(raft::NodeId id) {
    const std::uint64_t seq = nextSeq_++;
    return run(net::MembershipRequest{seq, false, id, {}}, seq);
}

Client::Result Client::run(const net::Packet& request, std::uint64_t seq) {
    const auto deadline = Clock::now() + options_.totalTimeout;
    std::string lastProblem = "no server answered";

    // Backoff after silence. Retrying instantly makes an overloaded cluster
    // worse (every retry is more work for it), so wait longer each time.
    auto silenceBackoff = kBackoff;

    while (Clock::now() < deadline) {
        net::ClientResponse response;
        if (!attempt(request, seq, response, deadline)) {
            lastProblem = "no answer from " + servers_[current_].toString();
            disconnect();
            useNextServer();
            std::this_thread::sleep_for(silenceBackoff);
            silenceBackoff = std::min(silenceBackoff * 2, kMaxBackoff);
            continue;
        }

        switch (response.status) {
            case net::ClientStatus::Ok:
                return Result{true, response.found, std::move(response.value), {}};

            case net::ClientStatus::TooLarge:
                return Result{false, false, {}, "the server refused the request as too large"};

            case net::ClientStatus::Rejected:
                return Result{false, false, {}, "the server rejected the request"};

            case net::ClientStatus::NotLeader:
                lastProblem = servers_[current_].toString() + " is not the leader";
                disconnect();
                if (auto leader = net::parseAddress(response.leaderAddr)) useServer(*leader);
                else useNextServer();
                std::this_thread::sleep_for(kBackoff);  // an election may be in progress
                break;

            case net::ClientStatus::Aborted:
                // Our entry was overwritten by a new leader: it did not happen.
                // Retrying (same seq) is safe.
                lastProblem = "the request was aborted by a leader change";
                std::this_thread::sleep_for(kBackoff);
                break;
        }
    }
    return Result{false, false, {}, "timed out (" + lastProblem + "); the operation may or may not have been applied"};
}

bool Client::attempt(const net::Packet& request, std::uint64_t seq, net::ClientResponse& response,
                     Clock::time_point deadline) {
    const auto attemptDeadline = std::min(deadline, Clock::now() + options_.attemptTimeout);
    if (!ensureConnected(attemptDeadline)) return false;

    if (!sendAll(fd_.get(), storage::frameRecord(net::encodePacket(request)))) return false;

    // Read until the answer to THIS request arrives. Answers to earlier
    // attempts (a slow leader replying after we gave up on it) have an older
    // seq and are skipped.
    while (true) {
        std::string payload;
        auto status = decoder_.next(payload);
        if (status == net::FrameDecoder::Status::Error) return false;
        if (status == net::FrameDecoder::Status::Frame) {
            net::Packet packet;
            if (!net::decodePacket(payload, packet)) return false;
            auto* resp = std::get_if<net::ClientResponse>(&packet);
            if (!resp) return false;
            if (resp->seq != seq) continue;
            response = std::move(*resp);
            return true;
        }

        if (!waitReadable(fd_.get(), attemptDeadline)) return false;
        char buf[16 * 1024];
        ssize_t n = ::recv(fd_.get(), buf, sizeof(buf), 0);
        if (n == 0) return false;
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        decoder_.feed(std::string_view(buf, static_cast<std::size_t>(n)));
    }
}

bool Client::ensureConnected(Clock::time_point deadline) {
    if (fd_.valid()) return true;
    auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now());
    if (left.count() <= 0) return false;
    fd_ = net::connectBlocking(servers_[current_], left);
    decoder_ = net::FrameDecoder();
    return fd_.valid();
}

void Client::disconnect() { fd_ = storage::UniqueFd(); }

void Client::useNextServer() { current_ = (current_ + 1) % servers_.size(); }

void Client::useServer(const net::Address& address) {
    auto it = std::find(servers_.begin(), servers_.end(), address);
    if (it == servers_.end()) {  // a node we weren't told about: remember it
        servers_.push_back(address);
        it = servers_.end() - 1;
    }
    current_ = static_cast<std::size_t>(it - servers_.begin());
}

}  // namespace kv::client
