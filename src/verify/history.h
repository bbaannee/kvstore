#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace kv::verify {

// One client operation as the client experienced it: when it started, when
// (if ever) it got an answer, and what the answer was.
struct Operation {
    enum class Type { Put, Get, Del };

    int client = 0;
    Type type = Type::Get;
    std::string key;
    std::string input;  // Put: the value written

    // A request whose answer never arrived (timeout, crash, partition) has
    // completed == false: it may have taken effect at any point after `call`,
    // or never. That is the "unknown outcome" of a distributed system, and
    // the checker treats it exactly that way.
    bool completed = false;
    bool found = false;  // Get/Del: did the key exist?
    std::string output;  // Get: the value that was returned

    // Logical timestamps from one shared counter: an operation A "happened
    // before" B only if A.ret < B.call. Operations whose intervals overlap
    // were concurrent, and may be ordered either way.
    std::uint64_t call = 0;
    std::uint64_t ret = 0;  // meaningless unless completed
};

// Records operations while a test runs. Thread-safe, so several client
// threads can share one History.
class History {
public:
    // Marks the start of an operation; returns its id for complete().
    std::size_t begin(int client, Operation::Type type, std::string key, std::string input = {});

    // Marks the operation as answered.
    void complete(std::size_t id, bool found, std::string output = {});

    // Drops an operation that is KNOWN not to have happened (the server
    // refused it before doing anything). Do not use it for a timeout: a
    // timed-out operation may well have taken effect.
    void cancel(std::size_t id);

    // Everything recorded so far (a copy), without cancelled operations.
    std::vector<Operation> operations() const;

private:
    mutable std::mutex mutex_;
    std::vector<Operation> operations_;
    std::vector<bool> cancelled_;
    std::uint64_t clock_ = 0;
};

}  // namespace kv::verify
