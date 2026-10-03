#include "verify/linearizability.h"

#include <map>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace kv::verify;

namespace {

Operation put(int client, std::string key, std::string value, std::uint64_t call, std::uint64_t ret) {
    return Operation{client, Operation::Type::Put, std::move(key), std::move(value), true, true, {}, call, ret};
}
Operation get(int client, std::string key, bool found, std::string value, std::uint64_t call, std::uint64_t ret) {
    return Operation{client, Operation::Type::Get, std::move(key), {}, true, found, std::move(value), call, ret};
}
Operation del(int client, std::string key, bool found, std::uint64_t call, std::uint64_t ret) {
    return Operation{client, Operation::Type::Del, std::move(key), {}, true, found, {}, call, ret};
}
Operation pendingPut(int client, std::string key, std::string value, std::uint64_t call) {
    return Operation{client, Operation::Type::Put, std::move(key), std::move(value), false, false, {}, call, 0};
}
Operation pendingDel(int client, std::string key, std::uint64_t call) {
    return Operation{client, Operation::Type::Del, std::move(key), {}, false, false, {}, call, 0};
}

bool ok(const std::vector<Operation>& h) {
    auto r = checkLinearizable(h);
    EXPECT_FALSE(r.inconclusive);
    return r.linearizable;
}

}  // namespace

TEST(Linearizability, EmptyAndSequentialHistoriesPass) {
    EXPECT_TRUE(ok({}));
    EXPECT_TRUE(ok({
        get(1, "x", false, "", 1, 2),
        put(1, "x", "1", 3, 4),
        get(1, "x", true, "1", 5, 6),
        put(1, "x", "2", 7, 8),
        get(1, "x", true, "2", 9, 10),
        del(1, "x", true, 11, 12),
        get(1, "x", false, "", 13, 14),
        del(1, "x", false, 15, 16),
    }));
}

TEST(Linearizability, ReadingAValueNobodyWroteFails) {
    EXPECT_FALSE(ok({put(1, "x", "1", 1, 2), get(2, "x", true, "9", 3, 4)}));
    EXPECT_FALSE(ok({get(1, "x", true, "1", 1, 2)}));
}

// The classic bug: a read that started AFTER a write finished must see it.
TEST(Linearizability, StaleReadFails) {
    EXPECT_FALSE(ok({
        put(1, "x", "1", 1, 2),
        put(1, "x", "2", 3, 4),
        get(2, "x", true, "1", 5, 6),  // x=2 was already acknowledged
    }));
    EXPECT_FALSE(ok({
        put(1, "x", "1", 1, 2),
        get(2, "x", false, "", 3, 4),  // "not found" after a completed put
    }));
}

// Overlapping operations can be ordered either way.
TEST(Linearizability, ConcurrentOperationsMayTakeEitherOrder) {
    // put and get overlap: the get may see the old or the new value.
    EXPECT_TRUE(ok({put(1, "x", "1", 1, 10), get(2, "x", false, "", 2, 5)}));
    EXPECT_TRUE(ok({put(1, "x", "1", 1, 10), get(2, "x", true, "1", 2, 5)}));
    // two concurrent puts: either can win, as long as everyone agrees afterwards.
    EXPECT_TRUE(ok({put(1, "x", "a", 1, 10), put(2, "x", "b", 2, 9), get(3, "x", true, "a", 11, 12)}));
    EXPECT_TRUE(ok({put(1, "x", "a", 1, 10), put(2, "x", "b", 2, 9), get(3, "x", true, "b", 11, 12)}));
}

// Once a read has seen a value, later reads can't go back in time.
TEST(Linearizability, ReadsCannotGoBackwardsInTime) {
    EXPECT_FALSE(ok({
        put(1, "x", "1", 1, 2),
        put(2, "x", "2", 3, 20),        // slow write, overlaps everything below
        get(3, "x", true, "2", 4, 5),   // saw the new value...
        get(3, "x", true, "1", 6, 7),   // ...then the old one again
    }));
    // The same reads in the opposite order are fine.
    EXPECT_TRUE(ok({
        put(1, "x", "1", 1, 2),
        put(2, "x", "2", 3, 20),
        get(3, "x", true, "1", 4, 5),
        get(3, "x", true, "2", 6, 7),
    }));
}

TEST(Linearizability, ConcurrentWritersMustAgreeOnOneOrder) {
    // Two readers disagree about the order of two concurrent puts.
    EXPECT_FALSE(ok({
        put(1, "x", "a", 1, 20),
        put(2, "x", "b", 1, 20),
        get(3, "x", true, "a", 2, 3),
        get(3, "x", true, "b", 4, 5),
        get(4, "x", true, "b", 2, 3),
        get(4, "x", true, "a", 4, 5),
    }));
}

TEST(Linearizability, DeleteResultsMustMatchTheState) {
    EXPECT_FALSE(ok({del(1, "x", true, 1, 2)}));                                  // nothing to delete
    EXPECT_FALSE(ok({put(1, "x", "1", 1, 2), del(1, "x", false, 3, 4)}));         // it was there
    EXPECT_FALSE(ok({put(1, "x", "1", 1, 2), del(1, "x", true, 3, 4), del(2, "x", true, 5, 6)}));
    EXPECT_TRUE(ok({put(1, "x", "1", 1, 2), del(1, "x", true, 3, 4), del(2, "x", false, 5, 6)}));
}

TEST(Linearizability, KeysAreIndependent) {
    EXPECT_TRUE(ok({
        put(1, "a", "1", 1, 2),
        put(1, "b", "2", 3, 4),
        get(2, "a", true, "1", 5, 6),
        get(2, "b", true, "2", 7, 8),
    }));
    auto bad = checkLinearizable({put(1, "a", "1", 1, 2), put(1, "b", "2", 3, 4), get(2, "b", true, "1", 5, 6)});
    EXPECT_FALSE(bad.linearizable);
    EXPECT_EQ(bad.key, "b");
    EXPECT_NE(bad.explanation.find("get(b)"), std::string::npos);
}

// An operation whose answer never arrived may have happened, or not.
TEST(Linearizability, PendingOperationsMayOrMayNotHaveTakenEffect) {
    // The put timed out, but a later read sees it: it did happen.
    EXPECT_TRUE(ok({pendingPut(1, "x", "1", 1), get(2, "x", true, "1", 5, 6)}));
    // Same put, and a later read doesn't see it: it didn't happen (yet, or ever).
    EXPECT_TRUE(ok({pendingPut(1, "x", "1", 1), get(2, "x", false, "", 5, 6)}));
    // It may even take effect after later reads: seen by the third, not the second.
    EXPECT_TRUE(ok({pendingPut(1, "x", "1", 1), get(2, "x", false, "", 5, 6), get(2, "x", true, "1", 7, 8)}));
    // A pending delete explains a value disappearing.
    EXPECT_TRUE(ok({put(1, "x", "1", 1, 2), pendingDel(2, "x", 3), get(3, "x", false, "", 10, 11)}));
    // But a pending operation can't explain a value nobody ever wrote...
    EXPECT_FALSE(ok({pendingPut(1, "x", "1", 1), get(2, "x", true, "2", 5, 6)}));
    // ...and can't take effect before it started.
    EXPECT_FALSE(ok({get(2, "x", true, "1", 1, 2), pendingPut(1, "x", "1", 5)}));
    // Once seen, a pending write stays: the register can't un-see it without a delete.
    EXPECT_FALSE(ok({pendingPut(1, "x", "1", 1), get(2, "x", true, "1", 5, 6), get(2, "x", false, "", 7, 8)}));
}

// Histories built from a sequential execution, with each operation's
// interval stretched around its linearization point, are linearizable by
// construction. The checker must accept all of them, and must reject them
// once a read is changed to a value nobody wrote.
TEST(Linearizability, RandomHistoriesBuiltFromASequentialRunPass) {
    for (std::uint64_t seed = 1; seed <= 200; ++seed) {
        std::mt19937_64 rng(seed);
        const int width = 1 + static_cast<int>(rng() % 6);  // how much operations overlap
        std::map<std::string, std::string> model;
        std::vector<Operation> history;

        for (int i = 1; i <= 60; ++i) {
            // The linearization point. The base keeps `point - spread` from going below zero.
            const std::uint64_t point = 1000 + static_cast<std::uint64_t>(i) * 10;
            Operation op;
            op.client = static_cast<int>(rng() % 4);
            op.key = std::string(1, static_cast<char>('a' + rng() % 3));
            op.call = point - rng() % (width * 10 + 1);
            op.ret = point + rng() % (width * 10 + 1);
            op.completed = rng() % 10 != 0;  // some never get an answer

            switch (rng() % 3) {
                case 0:
                    op.type = Operation::Type::Put;
                    op.input = "v" + std::to_string(i);
                    model[op.key] = op.input;
                    break;
                case 1: {
                    op.type = Operation::Type::Get;
                    auto it = model.find(op.key);
                    op.found = it != model.end();
                    if (op.found) op.output = it->second;
                    break;
                }
                default:
                    op.type = Operation::Type::Del;
                    op.found = model.erase(op.key) > 0;
            }
            history.push_back(op);
        }

        auto result = checkLinearizable(history);
        ASSERT_FALSE(result.inconclusive) << "seed " << seed;
        ASSERT_TRUE(result.linearizable) << "seed " << seed << "\n" << result.explanation;

        // Break it: a completed read now returns a value nobody wrote.
        for (Operation& op : history)
            if (op.completed && op.type == Operation::Type::Get) {
                op.found = true;
                op.output = "never-written";
                EXPECT_FALSE(checkLinearizable(history).linearizable) << "seed " << seed << ": corruption not detected";
                break;
            }
    }
}

TEST(Linearizability, HandlesHeavyConcurrencyWithinTheSearchBudget) {
    std::mt19937_64 rng(99);
    std::map<std::string, std::string> model;
    std::vector<Operation> history;
    for (int i = 1; i <= 150; ++i) {
        const std::uint64_t point = 1000 + static_cast<std::uint64_t>(i) * 4;
        Operation op;
        op.client = static_cast<int>(rng() % 8);
        op.key = i % 2 ? "x" : "y";
        op.call = point - rng() % 40;  // up to ~10 operations overlap
        op.ret = point + rng() % 40;
        op.completed = true;
        if (rng() % 2) {
            op.type = Operation::Type::Put;
            op.input = std::to_string(i);
            model[op.key] = op.input;
        } else {
            op.type = Operation::Type::Get;
            auto it = model.find(op.key);
            op.found = it != model.end();
            if (op.found) op.output = it->second;
        }
        history.push_back(op);
    }
    auto result = checkLinearizable(history);
    EXPECT_FALSE(result.inconclusive);
    EXPECT_TRUE(result.linearizable) << result.explanation;
}

TEST(History, RecordsCallsAndReturnsInOrder) {
    History h;
    auto a = h.begin(1, Operation::Type::Put, "x", "1");
    auto b = h.begin(2, Operation::Type::Get, "x");
    h.complete(b, false);
    h.complete(a, true);
    auto ops = h.operations();
    ASSERT_EQ(ops.size(), 2u);
    EXPECT_LT(ops[0].call, ops[1].call);
    EXPECT_LT(ops[1].ret, ops[0].ret);  // b returned first
    EXPECT_TRUE(ops[0].completed);
    EXPECT_NE(describe(ops[0]).find("put(x, 1)"), std::string::npos);
}
