#include "net/socket.h"

#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>

using namespace kv::net;

TEST(Address, ParsesHostAndPort) {
    EXPECT_EQ(parseAddress("127.0.0.1:7001"), (Address{"127.0.0.1", 7001}));
    EXPECT_EQ(parseAddress("localhost:80"), (Address{"localhost", 80}));
    EXPECT_EQ(parseAddress("0.0.0.0:0"), (Address{"0.0.0.0", 0}));
    Address a{"10.0.0.5", 9000};
    EXPECT_EQ(a.toString(), "10.0.0.5:9000");
}

TEST(Address, RejectsBadInput) {
    for (const char* bad : {"", "127.0.0.1", ":80", "host:", "host:abc", "host:80x", "host:65536", "host:-1", "host: 80"})
        EXPECT_FALSE(parseAddress(bad)) << bad;
}

TEST(Socket, ListenConnectAndSendBytes) {
    std::uint16_t port = 0;
    auto listener = listenTcp({"127.0.0.1", 0}, port);  // port 0: the OS picks a free one
    ASSERT_GT(port, 0);

    auto client = connectBlocking({"127.0.0.1", port}, std::chrono::seconds(2));
    ASSERT_TRUE(client.valid());

    // The listener is non-blocking, so wait for the connection to be pending.
    int server = -1;
    for (int i = 0; i < 100 && server < 0; ++i) {
        server = ::accept(listener.get(), nullptr, nullptr);
        if (server < 0) usleep(10'000);
    }
    ASSERT_GE(server, 0);

    ASSERT_EQ(::send(client.get(), "ping", 4, 0), 4);
    char buf[8] = {};
    ASSERT_EQ(::recv(server, buf, sizeof(buf), 0), 4);
    EXPECT_EQ(std::string(buf, 4), "ping");
    ::close(server);
}

TEST(Socket, ConnectToNothingFailsWithoutThrowing) {
    // Grab a free port, then close it so nothing is listening there.
    std::uint16_t port = 0;
    { auto l = listenTcp({"127.0.0.1", 0}, port); }
    EXPECT_FALSE(connectBlocking({"127.0.0.1", port}, std::chrono::milliseconds(500)).valid());
    EXPECT_FALSE(connectBlocking({"no.such.host.invalid", 1}, std::chrono::milliseconds(500)).valid());
}

TEST(Socket, ListeningTwiceOnTheSamePortFails) {
    std::uint16_t port = 0;
    auto first = listenTcp({"127.0.0.1", 0}, port);
    std::uint16_t again = 0;
    EXPECT_THROW(listenTcp({"127.0.0.1", port}, again), std::system_error);
}
