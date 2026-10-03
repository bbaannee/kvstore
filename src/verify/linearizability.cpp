#include "verify/linearizability.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace kv::verify {

namespace {

using Type = Operation::Type;
using State = std::optional<std::string>;  // the register's value; nullopt = key absent

// Searches for a valid order of one key's operations.
class KeyChecker {
public:
    KeyChecker(std::vector<Operation> ops, std::size_t maxStates) : ops_(std::move(ops)), maxStates_(maxStates) {
        // Pending reads can't change the register, and nothing depends on
        // their (missing) result: drop them, which shrinks the search a lot.
        std::erase_if(ops_, [](const Operation& op) { return !op.completed && op.type == Type::Get; });
        std::sort(ops_.begin(), ops_.end(), [](const Operation& a, const Operation& b) { return a.call < b.call; });

        n_ = ops_.size();
        words_ = (n_ + 63) / 64;

        // mustPrecede_[i] = the completed operations that finished before
        // operation i started. Real-time order forces them to come first.
        mustPrecede_.assign(n_, std::vector<std::uint64_t>(words_, 0));
        completed_.assign(words_, 0);
        for (std::size_t i = 0; i < n_; ++i) {
            if (ops_[i].completed) completed_[i / 64] |= 1ull << (i % 64);
            for (std::size_t j = 0; j < n_; ++j)
                if (j != i && ops_[j].completed && ops_[j].ret < ops_[i].call)
                    mustPrecede_[i][j / 64] |= 1ull << (j % 64);
        }
    }

    // True if some valid order exists.
    bool run() {
        std::vector<std::uint64_t> none(words_, 0);
        return search(none, std::nullopt);
    }

    bool exhausted() const { return exhausted_; }
    std::size_t explored() const { return explored_; }
    const std::vector<Operation>& operations() const { return ops_; }

private:
    bool allCompletedDone(const std::vector<std::uint64_t>& mask) const {
        for (std::size_t w = 0; w < words_; ++w)
            if (completed_[w] & ~mask[w]) return false;
        return true;
    }

    // Can operation i go next, given what has already been placed in the order?
    bool canGoNext(std::size_t i, const std::vector<std::uint64_t>& mask) const {
        for (std::size_t w = 0; w < words_; ++w)
            if (mustPrecede_[i][w] & ~mask[w]) return false;
        return true;
    }

    // What a sequential map would do. Returns false if the recorded result
    // contradicts the register's current value.
    static bool apply(const Operation& op, const State& state, State& next) {
        switch (op.type) {
            case Type::Put:
                next = op.input;
                return true;
            case Type::Del:
                next = std::nullopt;
                return !op.completed || op.found == state.has_value();
            case Type::Get:
                next = state;
                return !op.completed || (op.found == state.has_value() && (!op.found || op.output == *state));
        }
        return false;
    }

    static std::string memoKey(const std::vector<std::uint64_t>& mask, const State& state) {
        std::string key(reinterpret_cast<const char*>(mask.data()), mask.size() * sizeof(std::uint64_t));
        key += state ? '1' : '0';
        if (state) key += *state;
        return key;
    }

    bool search(const std::vector<std::uint64_t>& mask, const State& state) {
        if (allCompletedDone(mask)) return true;
        if (++explored_ > maxStates_) {
            exhausted_ = true;
            return false;
        }

        // A state we already proved is a dead end: don't search it again.
        // (Without this, k concurrent operations cost k! instead of 2^k.)
        std::string key = memoKey(mask, state);
        if (failed_.count(key)) return false;

        for (std::size_t i = 0; i < n_; ++i) {
            if (mask[i / 64] >> (i % 64) & 1) continue;  // already placed
            if (!canGoNext(i, mask)) continue;

            State next;
            if (!apply(ops_[i], state, next)) continue;

            std::vector<std::uint64_t> nextMask = mask;
            nextMask[i / 64] |= 1ull << (i % 64);
            if (search(nextMask, next)) return true;
            if (exhausted_) return false;
        }

        failed_.insert(std::move(key));
        return false;
    }

    std::vector<Operation> ops_;
    std::size_t maxStates_;
    std::size_t n_ = 0;
    std::size_t words_ = 0;
    std::vector<std::vector<std::uint64_t>> mustPrecede_;
    std::vector<std::uint64_t> completed_;
    std::unordered_set<std::string> failed_;
    std::size_t explored_ = 0;
    bool exhausted_ = false;
};

}  // namespace

std::string describe(const Operation& op) {
    std::ostringstream out;
    out << "client " << op.client << ": ";
    switch (op.type) {
        case Type::Put: out << "put(" << op.key << ", " << op.input << ")"; break;
        case Type::Del: out << "del(" << op.key << ")"; break;
        case Type::Get: out << "get(" << op.key << ")"; break;
    }
    if (!op.completed) {
        out << " -> no answer [call " << op.call << "]";
    } else {
        if (op.type == Type::Get) out << " -> " << (op.found ? op.output : "(not found)");
        if (op.type == Type::Del) out << " -> " << (op.found ? "deleted" : "(not found)");
        out << " [call " << op.call << ", ret " << op.ret << "]";
    }
    return out.str();
}

CheckResult checkLinearizable(const std::vector<Operation>& history, std::size_t maxStatesPerKey) {
    CheckResult result;

    std::map<std::string, std::vector<Operation>> byKey;  // keys are independent: check each alone
    for (const Operation& op : history) byKey[op.key].push_back(op);

    for (auto& [key, ops] : byKey) {
        KeyChecker checker(std::move(ops), maxStatesPerKey);
        const bool ok = checker.run();
        result.statesExplored += checker.explored();

        if (checker.exhausted()) {
            result.linearizable = false;
            result.inconclusive = true;
            result.key = key;
            result.explanation = "search limit reached on key '" + key + "'";
            return result;
        }
        if (!ok) {
            result.linearizable = false;
            result.key = key;
            std::ostringstream text;
            text << "no valid order exists for key '" << key << "'. Its operations:\n";
            for (const Operation& op : checker.operations()) text << "  " << describe(op) << "\n";
            result.explanation = text.str();
            return result;
        }
    }
    return result;
}

}  // namespace kv::verify
