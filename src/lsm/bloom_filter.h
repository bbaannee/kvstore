#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv::lsm {

// A fast 64-bit hash of `data`, different for every `seed`.
std::uint64_t hash64(std::string_view data, std::uint64_t seed);

// A Bloom filter: a compact summary of a set of keys that can answer "is this
// key definitely NOT in the set?". It never says "not present" for a key that
// is present (no false negatives), but sometimes says "maybe present" for a
// key that isn't (false positives, about 1% with 10 bits per key).
//
// An SSTable keeps one for its keys. A lookup asks the filter first, and skips
// reading the table's data blocks when the answer is "definitely not here":
// that's what keeps reads cheap when a key is spread over many tables.
//
// How it works: an array of bits, all zero. To add a key, compute k different
// hash values and set the k bits they point to. To test a key, check those k
// bits: if any is zero, the key was never added.
//
// The filter is a plain byte string: the bit array followed by one byte holding k.
class BloomFilter {
public:
    static std::string build(const std::vector<std::string>& keys, int bitsPerKey);

    // True if `key` MAY be in the set that `filter` was built from.
    static bool mayContain(std::string_view filter, std::string_view key);
};

}  // namespace kv::lsm
