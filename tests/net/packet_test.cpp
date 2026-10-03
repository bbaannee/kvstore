#include "net/packet.h"

#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace kv;
using namespace kv::net;

namespace {

raft::Entry entry(raft::Term term, raft::Index index, std::string data = "") {
    return raft::Entry{term, index, raft::Entry::Type::Command, std::move(data)};
}

// One packet of every kind, including awkward contents.
std::vector<Packet> samplePackets() {
    return {
        raft::Message{1, 2, 7, raft::RequestVote{10, 6}},
        raft::Message{2, 1, 7, raft::RequestVoteResponse{true}},
        raft::Message{2, 1, 8, raft::RequestVoteResponse{false}},
        raft::Message{1, 3, 9, raft::AppendEntries{4, 3, {}, 2}},  // heartbeat
        raft::Message{1, 3, 9,
                      raft::AppendEntries{4, 3,
                                          {entry(3, 5, "hello"), entry(9, 6, std::string("bin\0ary\n", 8)),
                                           raft::Entry{9, 7, raft::Entry::Type::NoOp, {}}},
                                          5}},
        raft::Message{3, 1, 9, raft::AppendEntriesResponse{true, 7, 0}},
        raft::Message{3, 1, 9, raft::AppendEntriesResponse{false, 0, 4}},
        raft::Message{1, 3, 9, raft::AppendEntries{4, 3, {entry(9, 5, "x")}, 5, 123456789}},  // with a heartbeat round
        raft::Message{3, 1, 9, raft::AppendEntriesResponse{true, 7, 0, 123456789}},
        raft::Message{1, 2, 4, raft::AppendEntries{0, 0, {raft::Entry{0, 1, raft::Entry::Type::Config, "cfg"}}, 0}},
        raft::Message{1, 4, 6, raft::InstallSnapshot{40, 5, {{1, "a:1"}, {2, "b:2"}}, 0, "first chunk", false}},
        raft::Message{1, 4, 6, raft::InstallSnapshot{40, 5, {{1, "a:1"}, {2, "b:2"}}, 11, std::string("last\0chunk", 10), true}},
        raft::Message{1, 4, 6, raft::InstallSnapshot{40, 5, {}, 0, "", true}},
        raft::Message{4, 1, 6, raft::InstallSnapshotResponse{40, 11, false}},
        raft::Message{4, 1, 6, raft::InstallSnapshotResponse{40, 0, true}},
        ClientRequest{42, 1, storage::LogRecord::put("name", "Bane Okolovic")},
        ClientRequest{~0ull, ~0ull, storage::LogRecord::del("k")},
        ClientRequest{1, 2, storage::LogRecord::get("")},
        ClientResponse{1, ClientStatus::Ok, true, "value with\nnewline", ""},
        ClientResponse{2, ClientStatus::NotLeader, false, "", "127.0.0.1:7002"},
        ClientResponse{3, ClientStatus::Aborted, false, "", ""},
        ClientResponse{4, ClientStatus::TooLarge, false, "", ""},
        ClientResponse{5, ClientStatus::Rejected, false, "", ""},
        Hello{3, "10.0.0.7:7003"},
        Hello{9, ""},
        MembershipRequest{1, true, 4, "127.0.0.1:7004"},
        MembershipRequest{2, false, 2, ""},
    };
}

}  // namespace

TEST(Packet, EveryKindRoundTrips) {
    for (const Packet& p : samplePackets()) {
        Packet out;
        ASSERT_TRUE(decodePacket(encodePacket(p), out));
        EXPECT_EQ(out, p);
    }
}

TEST(Packet, EncodingIsLittleEndianAndStable) {
    // Pin one packet's exact bytes so the wire format can't change by accident.
    Packet p = raft::Message{1, 2, 3, raft::RequestVote{4, 5}};
    EXPECT_EQ(encodePacket(p), std::string("\x01"                  // type: raft
                                           "\x01\0\0\0\0\0\0\0"    // from = 1
                                           "\x02\0\0\0\0\0\0\0"    // to = 2
                                           "\x03\0\0\0\0\0\0\0"    // term = 3
                                           "\x00"                  // kind: RequestVote
                                           "\x04\0\0\0\0\0\0\0"    // lastLogIndex = 4
                                           "\x05\0\0\0\0\0\0\0",   // lastLogTerm = 5
                                           1 + 24 + 1 + 16));
}

TEST(Packet, EveryTruncationIsRejected) {
    for (const Packet& p : samplePackets()) {
        const std::string full = encodePacket(p);
        for (std::size_t len = 0; len < full.size(); ++len) {
            Packet out;
            EXPECT_FALSE(decodePacket(std::string_view(full).substr(0, len), out)) << "length " << len;
        }
        Packet out;
        EXPECT_FALSE(decodePacket(full + "x", out)) << "trailing byte";
    }
}

TEST(Packet, RejectsInvalidFields) {
    Packet out;
    EXPECT_FALSE(decodePacket("", out));
    EXPECT_FALSE(decodePacket("\x09", out));  // unknown packet type

    std::string vote = encodePacket(raft::Message{1, 2, 3, raft::RequestVoteResponse{true}});
    vote.back() = '\x02';  // a bool must be 0 or 1
    EXPECT_FALSE(decodePacket(vote, out));

    std::string kind = encodePacket(raft::Message{1, 2, 3, raft::RequestVote{}});
    kind[25] = '\x09';  // unknown message kind
    EXPECT_FALSE(decodePacket(kind, out));

    std::string status = encodePacket(ClientResponse{1, ClientStatus::Ok, false, "", ""});
    status[9] = '\x07';  // unknown status
    EXPECT_FALSE(decodePacket(status, out));

    std::string badOp = encodePacket(ClientRequest{1, 1, storage::LogRecord::put("a", "b")});
    badOp[17] = '\x07';  // unknown operation type inside the request
    EXPECT_FALSE(decodePacket(badOp, out));
}

// A configuration on the wire must be sorted and duplicate-free, like in the log.
TEST(Packet, RejectsAnUnsortedMemberList) {
    std::string good = encodePacket(raft::Message{1, 2, 3, raft::InstallSnapshot{1, 1, {{1, ""}, {2, ""}}, 0, "", true}});
    Packet out;
    ASSERT_TRUE(decodePacket(good, out));

    // The two ids are 8 bytes each in the member list; swap them.
    std::string bad = good;
    const std::size_t membersAt = 1 + 24 + 1 + 16 + 4;  // type, envelope, kind, lastIndex+lastTerm, member count
    std::swap_ranges(bad.begin() + membersAt, bad.begin() + membersAt + 8, bad.begin() + membersAt + 12);
    EXPECT_FALSE(decodePacket(bad, out));
}

// Lengths and counts come from the network, so they must never be trusted:
// no allocation, no read past the end.
TEST(Packet, HugeCountsAndLengthsAreRejected) {
    Packet out;

    // AppendEntries claiming 2 billion entries in a tiny packet.
    std::string manyEntries = encodePacket(raft::Message{1, 2, 3, raft::AppendEntries{}});
    manyEntries.replace(manyEntries.size() - 4, 4, "\xFF\xFF\xFF\x7F", 4);
    EXPECT_FALSE(decodePacket(manyEntries, out));

    // An entry whose data length is bigger than the packet.
    std::string hugeData = encodePacket(raft::Message{1, 2, 3, raft::AppendEntries{0, 0, {entry(1, 1, "x")}, 0}});
    hugeData.replace(hugeData.size() - 5, 4, "\xFF\xFF\xFF\x7F", 4);
    EXPECT_FALSE(decodePacket(hugeData, out));

    // A response whose value length is bigger than the packet.
    std::string hugeValue = encodePacket(ClientResponse{1, ClientStatus::Ok, true, "v", ""});
    hugeValue.replace(11, 4, "\xFF\xFF\xFF\xFF", 4);
    EXPECT_FALSE(decodePacket(hugeValue, out));
}

// Throw damaged packets at the decoder. It may accept or reject them, but it
// must never crash or read out of bounds (run this under ASan to see that).
TEST(Packet, DecoderSurvivesRandomDamage) {
    std::mt19937_64 rng(1234);
    for (const Packet& p : samplePackets()) {
        const std::string good = encodePacket(p);
        for (int i = 0; i < 2000; ++i) {
            std::string bad = good;
            for (int flips = 1 + rng() % 3; flips > 0; --flips) bad[rng() % bad.size()] = static_cast<char>(rng());
            if (rng() % 4 == 0) bad.resize(rng() % bad.size());
            Packet out;
            decodePacket(bad, out);
        }
    }
    for (int i = 0; i < 5000; ++i) {  // pure noise
        std::string noise(rng() % 64, '\0');
        for (char& c : noise) c = static_cast<char>(rng());
        Packet out;
        decodePacket(noise, out);
    }
}
