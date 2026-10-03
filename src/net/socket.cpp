#include "net/socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstring>
#include <stdexcept>

namespace kv::net {

namespace {

// Resolves host:port to an IPv4 socket address. Uses getaddrinfo, which
// blocks for name lookups; fine for IP addresses and for startup.
sockaddr_in resolve(const Address& address) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    int rc = ::getaddrinfo(address.host.c_str(), std::to_string(address.port).c_str(), &hints, &result);
    if (rc != 0 || !result)
        throw std::runtime_error("cannot resolve " + address.toString() + ": " + ::gai_strerror(rc));
    sockaddr_in out{};
    std::memcpy(&out, result->ai_addr, sizeof(out));
    ::freeaddrinfo(result);
    return out;
}

storage::UniqueFd newSocket() {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) storage::throwErrno("socket");
    return storage::UniqueFd(fd);
}

}  // namespace

std::optional<Address> parseAddress(std::string_view text) {
    auto colon = text.rfind(':');
    if (colon == std::string_view::npos || colon == 0) return std::nullopt;

    unsigned port = 0;
    std::string_view portText = text.substr(colon + 1);
    auto [end, ec] = std::from_chars(portText.data(), portText.data() + portText.size(), port);
    if (ec != std::errc{} || end != portText.data() + portText.size() || portText.empty() || port > 65535)
        return std::nullopt;
    return Address{std::string(text.substr(0, colon)), static_cast<std::uint16_t>(port)};
}

void setNonBlocking(int fd) {
    int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) storage::throwErrno("fcntl O_NONBLOCK");
}

void setNoDelay(int fd) {
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));  // best effort
}

void setSendBufferSize(int fd, int bytes) {
    ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes));  // best effort
}

storage::UniqueFd listenTcp(const Address& address, std::uint16_t& boundPort) {
    storage::UniqueFd fd = newSocket();
    int one = 1;
    // Lets a restarted server rebind its port right away instead of waiting
    // for old connections to finish TIME_WAIT.
    ::setsockopt(fd.get(), SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr = resolve(address);
    if (::bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
        storage::throwErrno("bind " + address.toString());
    if (::listen(fd.get(), 128) != 0) storage::throwErrno("listen");

    sockaddr_in actual{};
    socklen_t len = sizeof(actual);
    if (::getsockname(fd.get(), reinterpret_cast<sockaddr*>(&actual), &len) != 0) storage::throwErrno("getsockname");
    boundPort = ntohs(actual.sin_port);
    return fd;
}

storage::UniqueFd startConnect(const Address& address, bool& inProgress) {
    storage::UniqueFd fd = newSocket();
    setNoDelay(fd.get());
    sockaddr_in addr = resolve(address);
    int rc = ::connect(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc == 0) {
        inProgress = false;
    } else if (errno == EINPROGRESS) {
        inProgress = true;
    } else {
        storage::throwErrno("connect " + address.toString());
    }
    return fd;
}

int connectError(int fd) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) return errno;
    return err;
}

storage::UniqueFd connectBlocking(const Address& address, std::chrono::milliseconds timeout) {
    try {
        bool inProgress = false;
        storage::UniqueFd fd = startConnect(address, inProgress);
        if (inProgress) {
            pollfd p{fd.get(), POLLOUT, 0};
            int rc;
            do {
                rc = ::poll(&p, 1, static_cast<int>(timeout.count()));
            } while (rc < 0 && errno == EINTR);
            if (rc <= 0 || connectError(fd.get()) != 0) return {};
        }
        int flags = ::fcntl(fd.get(), F_GETFL, 0);
        ::fcntl(fd.get(), F_SETFL, flags & ~O_NONBLOCK);
        return fd;
    } catch (const std::exception&) {
        return {};  // unresolvable name, refused, ...: the caller just tries the next server
    }
}

}  // namespace kv::net
