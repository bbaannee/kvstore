// End-to-end tests over real TCP: servers with event loops on localhost
// ports, talking to each other and to the client library.

#include "server/node_server.h"

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "client/client.h"
#include "live_cluster.h"
#include "net/limits.h"
#include "storage/log_format.h"

using kv::client::Client;
using kv::test::LiveCluster;
using namespace std::chrono_literals;

namespace {

Client makeClient(const std::vector<std::string>& servers) {
    Client::Options o;
    o.servers = servers;
    o.totalTimeout = 15s;
    return Client(o);
}

// A hand-rolled request over a raw socket, so tests can send exactly the
// bytes they want (a duplicate, garbage, ...) without the client library
// being clever. Tries every server until one is the leader and answers.
// One request to one server; returns its answer whatever it says, or nothing
// if the server didn't answer.
std::optional<kv::net::ClientResponse> exchange(const kv::net::Address& addr, const kv::net::ClientRequest& req) {
    auto fd = kv::net::connectBlocking(addr, 500ms);
    if (!fd.valid()) return std::nullopt;
    std::string frame = kv::storage::frameRecord(kv::net::encodePacket(req));
    std::size_t sent = 0;
    while (sent < frame.size()) {
        ssize_t n = ::send(fd.get(), frame.data() + sent, frame.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) return std::nullopt;
        sent += static_cast<std::size_t>(n);
    }

    kv::net::FrameDecoder decoder;
    std::string payload;
    timeval tv{2, 0};
    ::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    while (decoder.next(payload) == kv::net::FrameDecoder::Status::NeedMore) {
        char buf[4096];
        ssize_t n = ::recv(fd.get(), buf, sizeof(buf), 0);
        if (n <= 0) return std::nullopt;
        decoder.feed(std::string_view(buf, static_cast<std::size_t>(n)));
    }
    kv::net::Packet packet;
    if (payload.empty() || !kv::net::decodePacket(payload, packet)) return std::nullopt;
    auto* resp = std::get_if<kv::net::ClientResponse>(&packet);
    return resp ? std::optional(*resp) : std::nullopt;
}

// Keeps asking until the leader answers Ok.
kv::net::ClientResponse rawRequest(const std::vector<kv::net::Address>& servers, const kv::net::ClientRequest& req) {
    const auto deadline = std::chrono::steady_clock::now() + 15s;
    while (std::chrono::steady_clock::now() < deadline) {
        for (const auto& addr : servers) {
            auto resp = exchange(addr, req);
            if (resp && resp->status == kv::net::ClientStatus::Ok) return *resp;
        }
        std::this_thread::sleep_for(50ms);
    }
    ADD_FAILURE() << "no server answered the request";
    return {};
}

}  // namespace

TEST(NodeServer, ClientCanPutGetAndDelete) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());

    ASSERT_TRUE(client.put("name", "Bane").ok);
    auto got = client.get("name");
    ASSERT_TRUE(got.ok);
    EXPECT_TRUE(got.found);
    EXPECT_EQ(got.value, "Bane");

    auto overwritten = client.put("name", "someone else");
    EXPECT_TRUE(overwritten.ok);
    EXPECT_EQ(client.get("name").value, "someone else");

    auto deleted = client.del("name");
    EXPECT_TRUE(deleted.ok);
    EXPECT_TRUE(deleted.found);
    EXPECT_FALSE(client.get("name").found);
    EXPECT_FALSE(client.del("name").found);  // already gone
}

TEST(NodeServer, ValuesCanContainAnyBytes) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());

    const std::string awkward("line one\nline two\0 and a NUL, plus spaces  ", 43);
    ASSERT_TRUE(client.put("key with spaces", awkward).ok);
    EXPECT_EQ(client.get("key with spaces").value, awkward);

    ASSERT_TRUE(client.put("", "empty key").ok);
    EXPECT_EQ(client.get("").value, "empty key");
    ASSERT_TRUE(client.put("empty value", "").ok);
    EXPECT_TRUE(client.get("empty value").found);
}

// The biggest allowed value, through sockets whose send buffers are only a
// few KB. A single send() then takes a small part of the message, so the
// server must remember the rest and finish sending when the socket becomes
// writable again. That path is the same for every large message; the tiny
// buffers just make it happen at a size that doesn't stall the test.
//
// The slow transfer needs a slow clock: with the usual fast ticks (election
// timeout 100-200 ms) a follower whose leader's heartbeats are stuck behind
// the big message starts an election, the new leader re-sends the big entry,
// and the cluster keeps electing instead of committing. (We saw exactly that:
// 16 terms in 15 seconds.) 50 ms ticks give the realistic 0.5-1 s timeout.
TEST(NodeServer, LargestValueSurvivesPartialWrites) {
    LiveCluster cluster(3, /*sendBufferBytes=*/4096, /*tickMs=*/50);
    Client client = makeClient(cluster.serverList());

    std::string big(kv::net::kMaxValueSize, 'x');
    for (std::size_t i = 0; i < big.size(); i += 4099) big[i] = static_cast<char>('a' + i % 26);  // not all alike
    ASSERT_TRUE(client.put("big", big).ok);
    auto got = client.get("big");
    ASSERT_TRUE(got.ok);
    EXPECT_TRUE(got.value == big);  // (EXPECT_EQ would print 1 MB on failure)
}

TEST(NodeServer, OversizedRequestsAreRefused) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());

    // The client library refuses up front...
    auto tooBig = client.put("k", std::string(kv::net::kMaxValueSize + 1, 'x'));
    EXPECT_FALSE(tooBig.ok);
    EXPECT_NE(tooBig.error.find("maximum"), std::string::npos) << tooBig.error;
    EXPECT_FALSE(client.put(std::string(kv::net::kMaxKeySize + 1, 'k'), "v").ok);

    // ...and so does the server, if some other client doesn't.
    for (const auto& addr : cluster.addressList()) {
        auto resp = exchange(addr, kv::net::ClientRequest{77, 1, kv::storage::LogRecord::put(
                                       "k", std::string(kv::net::kMaxValueSize + 1, 'x'))});
        ASSERT_TRUE(resp) << addr.toString();
        EXPECT_EQ(resp->status, kv::net::ClientStatus::TooLarge) << addr.toString();
    }
    EXPECT_FALSE(client.get("k").found);  // nothing was stored
}

// Only the leader takes requests. A client that happens to start at a
// follower must be sent to the leader (the follower tells it who that is).
TEST(NodeServer, FollowersRedirectClientsToTheLeader) {
    LiveCluster cluster(3);
    for (auto id : cluster.ids()) {
        // A client that knows only ONE server. Two of the three are followers,
        // so for them the only way to succeed is to follow the leader hint.
        Client client = makeClient({cluster.address(id).toString()});
        auto key = "via-node-" + std::to_string(id);
        ASSERT_TRUE(client.put(key, "ok").ok) << "client starting at node " << id;
        EXPECT_EQ(client.get(key).value, "ok");
    }
}

// Kill every node in turn, the leader included at some point. The cluster
// must keep serving with 2 of 3 nodes, and a restarted node must catch up.
TEST(NodeServer, SurvivesEveryNodeBeingRestartedInTurn) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());
    std::map<std::string, std::string> expected;

    for (auto victim : cluster.ids()) {
        auto before = "before-" + std::to_string(victim);
        ASSERT_TRUE(client.put(before, "1").ok);
        expected[before] = "1";

        cluster.stop(victim);

        auto during = "during-" + std::to_string(victim);
        auto r = client.put(during, "2");  // may need a new election first
        ASSERT_TRUE(r.ok) << "with node " << victim << " down: " << r.error;
        expected[during] = "2";
        for (const auto& [k, v] : expected) EXPECT_EQ(client.get(k).value, v) << k;

        cluster.restart(victim);
    }

    // Let the restarted nodes hear from the leader, then compare everyone's state.
    ASSERT_TRUE(client.put("last", "3").ok);
    expected["last"] = "3";
    std::this_thread::sleep_for(500ms);
    cluster.stopAll();

    for (auto id : cluster.ids()) {
        const auto& data = cluster.node(id).state().data();
        const std::map<std::string, std::string> actual(data.begin(), data.end());
        EXPECT_EQ(actual, expected) << "node " << id;
    }
}

TEST(NodeServer, WholeClusterRestartKeepsTheData) {
    LiveCluster cluster(3);
    {
        Client client = makeClient(cluster.serverList());
        ASSERT_TRUE(client.put("durable", "yes").ok);
    }
    for (auto id : cluster.ids()) cluster.restart(id);

    Client client = makeClient(cluster.serverList());
    auto got = client.get("durable");
    ASSERT_TRUE(got.ok) << got.error;
    EXPECT_EQ(got.value, "yes");
}

// The retry guarantee, at the protocol level: sending the same
// (clientId, seq) twice executes it once and returns the same answer.
TEST(NodeServer, ADuplicateRequestRunsOnlyOnce) {
    LiveCluster cluster(3);
    const auto servers = cluster.addressList();
    using kv::net::ClientRequest;
    using kv::storage::LogRecord;

    EXPECT_TRUE(rawRequest(servers, ClientRequest{5, 1, LogRecord::put("a", "1")}).found);

    ClientRequest del{5, 2, LogRecord::del("a")};
    auto first = rawRequest(servers, del);
    EXPECT_TRUE(first.found);  // "a" existed

    // The same Del again, as if our first answer had been lost. Run for
    // real it would say "not found"; deduplicated it repeats the answer.
    auto second = rawRequest(servers, del);
    EXPECT_TRUE(second.found);

    // A delayed duplicate of an OLD request must not undo newer writes.
    EXPECT_TRUE(rawRequest(servers, ClientRequest{5, 3, LogRecord::put("a", "new")}).found);
    rawRequest(servers, ClientRequest{5, 1, LogRecord::put("a", "stale")});
    auto now = rawRequest(servers, ClientRequest{6, 1, LogRecord::get("a")});
    EXPECT_EQ(now.value, "new");
}

// Garbage on a connection closes THAT connection and nothing else.
TEST(NodeServer, GarbageBytesCloseOnlyTheOffendingConnection) {
    LiveCluster cluster(3);
    Client client = makeClient(cluster.serverList());
    ASSERT_TRUE(client.put("k", "v").ok);

    for (const auto& addr : cluster.addressList()) {
        for (const std::string& garbage :
             {std::string("GET / HTTP/1.1\r\n\r\n"), std::string(100, '\xFF'), kv::storage::frameRecord("not a packet")}) {
            auto fd = kv::net::connectBlocking(addr, 1s);
            ASSERT_TRUE(fd.valid());
            ASSERT_EQ(::send(fd.get(), garbage.data(), garbage.size(), MSG_NOSIGNAL), static_cast<ssize_t>(garbage.size()));

            // The server hangs up on us.
            timeval tv{2, 0};
            ::setsockopt(fd.get(), SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            char buf[64];
            EXPECT_EQ(::recv(fd.get(), buf, sizeof(buf), 0), 0) << "server didn't close the connection";
        }
    }
    EXPECT_EQ(client.get("k").value, "v");  // the cluster is unaffected
}

TEST(NodeServer, ManyClientsAtOnce) {
    LiveCluster cluster(3);
    const auto servers = cluster.serverList();

    constexpr int kClients = 6, kOpsEach = 25;
    std::vector<std::thread> threads;
    std::vector<int> failures(kClients, 0);
    for (int c = 0; c < kClients; ++c) {
        threads.emplace_back([&, c] {
            Client client = makeClient(servers);
            for (int i = 0; i < kOpsEach; ++i) {
                auto key = "c" + std::to_string(c) + "-" + std::to_string(i);
                if (!client.put(key, key).ok || client.get(key).value != key) ++failures[c];
            }
        });
    }
    for (auto& t : threads) t.join();
    for (int c = 0; c < kClients; ++c) EXPECT_EQ(failures[c], 0) << "client " << c;

    Client checker = makeClient(servers);
    EXPECT_EQ(checker.get("c0-0").value, "c0-0");
    EXPECT_EQ(checker.get("c5-24").value, "c5-24");
}

TEST(Client, TimesOutWhenNoServerIsReachable) {
    // Two ports with nothing listening on them.
    std::vector<std::string> servers;
    for (int i = 0; i < 2; ++i) {
        std::uint16_t port = 0;
        { auto l = kv::net::listenTcp({"127.0.0.1", 0}, port); }
        servers.push_back("127.0.0.1:" + std::to_string(port));
    }
    Client::Options o;
    o.servers = servers;
    o.totalTimeout = 600ms;
    Client client(o);

    auto r = client.put("k", "v");
    EXPECT_FALSE(r.ok);
    EXPECT_NE(r.error.find("may or may not"), std::string::npos) << r.error;  // honest about the unknown outcome
}

// A retry must reuse the sequence number, or the cluster can't tell it from a
// new operation and might run it twice. Here a fake server swallows the first
// request without answering (like a leader that crashed right after
// committing), so the client has to retry, and we look at what it sends.
TEST(Client, RetriesWithTheSameSequenceNumber) {
    std::uint16_t port = 0;
    auto listener = kv::net::listenTcp({"127.0.0.1", 0}, port);

    std::vector<kv::net::ClientRequest> received;
    std::thread fake([&] {
        for (int connection = 0; connection < 2; ++connection) {
            pollfd p{listener.get(), POLLIN, 0};
            if (::poll(&p, 1, 5000) <= 0) return;
            int fd = ::accept(listener.get(), nullptr, nullptr);
            kv::net::FrameDecoder decoder;
            std::string payload;
            while (decoder.next(payload) == kv::net::FrameDecoder::Status::NeedMore) {
                char buf[4096];
                ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                if (n <= 0) break;
                decoder.feed(std::string_view(buf, static_cast<std::size_t>(n)));
            }
            kv::net::Packet packet;
            if (!kv::net::decodePacket(payload, packet)) { ::close(fd); return; }
            auto req = std::get<kv::net::ClientRequest>(packet);
            received.push_back(req);

            if (connection == 1) {  // second time: answer properly
                kv::net::ClientResponse resp{req.seq, kv::net::ClientStatus::Ok, true, "", ""};
                std::string frame = kv::storage::frameRecord(kv::net::encodePacket(resp));
                ::send(fd, frame.data(), frame.size(), MSG_NOSIGNAL);
                usleep(100'000);  // let the client read it before we hang up
            }
            ::close(fd);  // first time: hang up without answering
        }
    });

    Client::Options o;
    o.servers = {"127.0.0.1:" + std::to_string(port)};
    o.attemptTimeout = 500ms;
    o.totalTimeout = 10s;
    Client client(o);
    EXPECT_TRUE(client.put("k", "v").ok);
    fake.join();

    ASSERT_EQ(received.size(), 2u);
    EXPECT_EQ(received[0].clientId, received[1].clientId);
    EXPECT_EQ(received[0].seq, received[1].seq) << "a retry must reuse the sequence number";
    EXPECT_TRUE(received[0].op == received[1].op);
}

// Retrying instantly makes an overloaded cluster worse (every retry is more
// work for it), so the client waits longer after each silent attempt. A fake
// server that never answers lets us count how often the client tries.
TEST(Client, BacksOffWhileTheClusterIsSilent) {
    std::uint16_t port = 0;
    auto listener = kv::net::listenTcp({"127.0.0.1", 0}, port);

    std::atomic<bool> done{false};
    std::atomic<int> attempts{0};
    std::vector<kv::storage::UniqueFd> held;  // keep connections open and silent
    std::thread silent([&] {
        while (!done) {
            pollfd p{listener.get(), POLLIN, 0};
            if (::poll(&p, 1, 50) > 0) {
                int fd = ::accept(listener.get(), nullptr, nullptr);
                if (fd >= 0) {
                    held.emplace_back(fd);
                    ++attempts;
                }
            }
        }
    });

    Client::Options o;
    o.servers = {"127.0.0.1:" + std::to_string(port)};
    o.attemptTimeout = 100ms;
    o.totalTimeout = 1800ms;
    Client client(o);
    EXPECT_FALSE(client.put("k", "v").ok);
    done = true;
    silent.join();

    // A fixed 30 ms pause would allow ~13 attempts in 1.8 s; doubling the
    // pause (30, 60, 120, ...) allows about 5.
    EXPECT_GE(attempts.load(), 3);
    EXPECT_LE(attempts.load(), 8) << "the client hammers a silent cluster";
}

TEST(Client, EachNewOperationGetsTheNextSequenceNumber) {
    LiveCluster cluster(1);
    // Two operations from the same client are two different requests: if they
    // shared a seq, the second would be mistaken for a retry of the first and
    // answered with the first one's result.
    Client client = makeClient(cluster.serverList());
    ASSERT_TRUE(client.put("a", "1").ok);
    EXPECT_FALSE(client.get("never-written").found);  // would repeat the Put's "found = true" if seqs were reused
    EXPECT_EQ(client.get("a").value, "1");
}

TEST(Client, RejectsAnEmptyServerList) {
    Client::Options o;
    o.servers = {"not an address"};
    EXPECT_THROW(Client{o}, std::invalid_argument);
}
