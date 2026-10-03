#include "net/frame_decoder.h"

#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "common/coding.h"
#include "common/crc32c.h"
#include "storage/log_format.h"

using kv::net::FrameDecoder;
using kv::storage::frameRecord;

namespace {

// Feeds everything, then returns every frame that came out.
std::vector<std::string> drain(FrameDecoder& d) {
    std::vector<std::string> out;
    std::string payload;
    while (d.next(payload) == FrameDecoder::Status::Frame) out.push_back(payload);
    return out;
}

}  // namespace

TEST(FrameDecoder, DecodesOneFrame) {
    FrameDecoder d;
    d.feed(frameRecord("hello"));
    std::string p;
    ASSERT_EQ(d.next(p), FrameDecoder::Status::Frame);
    EXPECT_EQ(p, "hello");
    EXPECT_EQ(d.next(p), FrameDecoder::Status::NeedMore);
}

TEST(FrameDecoder, EmptyAndBinaryPayloads) {
    FrameDecoder d;
    d.feed(frameRecord(""));
    d.feed(frameRecord(std::string("a\0b\n", 4)));
    EXPECT_EQ(drain(d), (std::vector<std::string>{"", std::string("a\0b\n", 4)}));
}

// TCP can deliver a byte at a time...
TEST(FrameDecoder, ByteByByte) {
    const std::string stream = frameRecord("first") + frameRecord("second");
    FrameDecoder d;
    std::vector<std::string> got;
    for (char c : stream) {
        d.feed(std::string_view(&c, 1));
        for (auto& f : drain(d)) got.push_back(f);
    }
    EXPECT_EQ(got, (std::vector<std::string>{"first", "second"}));
}

// ...or glued, cut at any point.
TEST(FrameDecoder, SplitAtEveryPosition) {
    const std::string stream = frameRecord("first") + frameRecord("second") + frameRecord("third");
    for (std::size_t cut = 0; cut <= stream.size(); ++cut) {
        FrameDecoder d;
        d.feed(std::string_view(stream).substr(0, cut));
        auto got = drain(d);
        d.feed(std::string_view(stream).substr(cut));
        for (auto& f : drain(d)) got.push_back(f);
        EXPECT_EQ(got, (std::vector<std::string>{"first", "second", "third"})) << "cut at " << cut;
    }
}

// Many frames through one decoder, so its buffer is compacted along the way.
TEST(FrameDecoder, ManyFramesInChunks) {
    std::string stream;
    std::vector<std::string> expected;
    for (int i = 0; i < 3000; ++i) {
        expected.push_back("message number " + std::to_string(i) + std::string(i % 50, 'x'));
        stream += frameRecord(expected.back());
    }
    FrameDecoder d;
    std::vector<std::string> got;
    for (std::size_t pos = 0; pos < stream.size(); pos += 1000) {
        d.feed(std::string_view(stream).substr(pos, 1000));
        for (auto& f : drain(d)) got.push_back(f);
    }
    EXPECT_EQ(got, expected);
}

// Any damage on a connection is an error. A single flipped bit anywhere in a
// complete frame must never produce a wrong payload.
TEST(FrameDecoder, EveryBitFlipIsDetected) {
    const std::string good = frameRecord("some payload");
    for (std::size_t i = 0; i < good.size(); ++i) {
        for (int bit = 0; bit < 8; ++bit) {
            std::string bad = good;
            bad[i] = static_cast<char>(bad[i] ^ (1 << bit));
            FrameDecoder d;
            d.feed(bad);
            std::string p;
            EXPECT_EQ(d.next(p), FrameDecoder::Status::Error) << "byte " << i << " bit " << bit;
        }
    }
}

TEST(FrameDecoder, ErrorIsPermanent) {
    FrameDecoder d;
    std::string bad = frameRecord("x");
    bad[0] ^= 0x01;
    d.feed(bad);
    std::string p;
    ASSERT_EQ(d.next(p), FrameDecoder::Status::Error);
    EXPECT_FALSE(d.error().empty());

    d.feed(frameRecord("valid after damage"));
    EXPECT_EQ(d.next(p), FrameDecoder::Status::Error);  // the stream can't be trusted anymore
}

// A damaged length must be caught right away, not make us wait for (or
// buffer) a gigantic payload that will never come.
TEST(FrameDecoder, OversizedLengthIsRejectedImmediately) {
    std::string header;
    std::string length;
    kv::putFixed32(length, 1u << 30);  // a valid length field (its checksum is right) but too big
    header += length;
    kv::putFixed32(header, kv::crc32c(length));
    kv::putFixed32(header, 0);

    FrameDecoder d(/*maxPayload=*/1024);
    d.feed(header);  // only the header: no payload follows
    std::string p;
    EXPECT_EQ(d.next(p), FrameDecoder::Status::Error);
}

TEST(FrameDecoder, WaitsForTheWholePayload) {
    const std::string frame = frameRecord(std::string(1000, 'x'));
    FrameDecoder d;
    d.feed(std::string_view(frame).substr(0, frame.size() - 1));
    std::string p;
    EXPECT_EQ(d.next(p), FrameDecoder::Status::NeedMore);
    d.feed(std::string_view(frame).substr(frame.size() - 1));
    EXPECT_EQ(d.next(p), FrameDecoder::Status::Frame);
    EXPECT_EQ(p.size(), 1000u);
}
