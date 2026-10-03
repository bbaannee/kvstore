#pragma once

#include <cstddef>

namespace kv::net {

// The largest key and value a client may use. Real stores cap request size
// too (etcd: about 1.5 MB), for good reasons:
//   - everything runs on one thread per node, so one huge request would
//     stall heartbeats, and the cluster would start electing new leaders;
//   - every request is copied, checksummed and written to disk on every
//     node, so cost grows with size;
//   - a request is retried whole when its answer is lost.
// A bigger value belongs in a blob store, with the KV store holding a pointer.
inline constexpr std::size_t kMaxKeySize = 4 * 1024;
inline constexpr std::size_t kMaxValueSize = 1024 * 1024;

}  // namespace kv::net
