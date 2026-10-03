#include "lsm/bloom_filter.h"

#include <algorithm>

namespace kv::lsm {

std::uint64_t hash64(std::string_view data, std::uint64_t seed) {
    // FNV-1a over the bytes, then a finalizer that spreads every input bit over the whole result.
    std::uint64_t h = 1469598103934665603ull ^ (seed * 0x9E3779B97F4A7C15ull);
    for (unsigned char c : data) {
        h ^= c;
        h *= 1099511628211ull;
    }
    h ^= h >> 30;
    h *= 0xBF58476D1CE4E5B9ull;
    h ^= h >> 27;
    h *= 0x94D049BB133111EBull;
    h ^= h >> 31;
    return h;
}

namespace {

constexpr int kMaxHashes = 30;

// The k bit positions of a key, by double hashing: position_i = h1 + i * h2.
// Two real hashes are enough to imitate k independent ones.
struct Probe {
    std::uint64_t h1, h2;
    explicit Probe(std::string_view key) : h1(hash64(key, 1)), h2(hash64(key, 2) | 1) {}
    std::uint64_t at(int i, std::uint64_t bits) const { return (h1 + static_cast<std::uint64_t>(i) * h2) % bits; }
};

}  // namespace

std::string BloomFilter::build(const std::vector<std::string>& keys, int bitsPerKey) {
    // The best number of hashes is bitsPerKey * ln 2.
    const int k = std::clamp(static_cast<int>(bitsPerKey * 0.69), 1, kMaxHashes);
    const std::size_t bytes = (std::max<std::size_t>(64, keys.size() * static_cast<std::size_t>(bitsPerKey)) + 7) / 8;
    const std::uint64_t bits = bytes * 8;

    std::string filter(bytes, '\0');
    for (const std::string& key : keys) {
        Probe probe(key);
        for (int i = 0; i < k; ++i) {
            const std::uint64_t bit = probe.at(i, bits);
            filter[bit / 8] = static_cast<char>(filter[bit / 8] | (1 << (bit % 8)));
        }
    }
    filter += static_cast<char>(k);
    return filter;
}

bool BloomFilter::mayContain(std::string_view filter, std::string_view key) {
    if (filter.size() < 2) return true;  // no usable filter: assume the key may be there
    const int k = static_cast<unsigned char>(filter.back());
    if (k < 1 || k > kMaxHashes) return true;
    const std::uint64_t bits = (filter.size() - 1) * 8;

    Probe probe(key);
    for (int i = 0; i < k; ++i) {
        const std::uint64_t bit = probe.at(i, bits);
        if (!(static_cast<unsigned char>(filter[bit / 8]) & (1 << (bit % 8)))) return false;
    }
    return true;
}

}  // namespace kv::lsm
