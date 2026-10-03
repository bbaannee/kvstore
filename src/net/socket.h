#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "storage/file_util.h"

namespace kv::net {

struct Address {
    std::string host;  // an IPv4 address or a name that resolves to one
    std::uint16_t port = 0;

    std::string toString() const { return host + ":" + std::to_string(port); }
    bool operator==(const Address&) const = default;
};

// Parses "host:port". Returns nullopt for anything else.
std::optional<Address> parseAddress(std::string_view text);

void setNonBlocking(int fd);
void setNoDelay(int fd);  // send small messages immediately instead of waiting to batch them

// Shrinks the kernel's send buffer. Only useful in tests: with a tiny
// buffer, send() accepts just a few KB at a time, which forces the
// "partial write, finish later" path that big messages take in real life.
void setSendBufferSize(int fd, int bytes);

// Opens a non-blocking listening socket. If address.port is 0 the OS picks
// a free port; `boundPort` receives the port actually used. Throws on failure.
storage::UniqueFd listenTcp(const Address& address, std::uint16_t& boundPort);

// Starts a non-blocking connect. `inProgress` is true if the connection
// isn't established yet: wait for the socket to become writable, then call
// connectError(). Throws if the connect can't even be started.
storage::UniqueFd startConnect(const Address& address, bool& inProgress);

// After a non-blocking connect completes: 0 on success, else an errno.
int connectError(int fd);

// A blocking connect with a timeout, for the client library. The returned
// socket is in blocking mode; an invalid fd means it failed or timed out.
storage::UniqueFd connectBlocking(const Address& address, std::chrono::milliseconds timeout);

}  // namespace kv::net
