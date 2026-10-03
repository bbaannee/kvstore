#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "verify/history.h"

namespace kv::verify {

struct CheckResult {
    bool linearizable = true;

    // True if the search gave up (too many states) before reaching a verdict.
    // `linearizable` is then meaningless; tests should treat it as a failure.
    bool inconclusive = false;

    // When !linearizable: the key whose operations have no valid order, and
    // those operations in a readable form.
    std::string key;
    std::string explanation;

    std::size_t statesExplored = 0;
};

// Decides whether a history of Put/Get/Del operations on a key-value store is
// LINEARIZABLE: is there a single order of all the operations, consistent
// with real time (if A finished before B started, A comes first), in which
// every operation returns what a plain sequential map would return?
//
// That is the guarantee a replicated store should give: it behaves as if it
// were one machine, even with crashes, partitions and retries behind the
// scenes. If this returns false, there is a bug, and `explanation` shows which
// operations cannot be explained.
//
// Keys are independent registers, so each key is checked separately
// (linearizability is "local"). Per key it does a depth-first search over
// the possible orders, with memoization, in the style of Wing & Gong and
// Lowe. Operations without an answer (completed == false) may have taken
// effect at any time after they started, or never.
CheckResult checkLinearizable(const std::vector<Operation>& history, std::size_t maxStatesPerKey = 5'000'000);

// One line describing an operation, e.g. "client 2: put(x, 7) [call 5, ret 9]".
std::string describe(const Operation& op);

}  // namespace kv::verify
