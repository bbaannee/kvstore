#include "verify/history.h"

namespace kv::verify {

std::size_t History::begin(int client, Operation::Type type, std::string key, std::string input) {
    std::lock_guard lock(mutex_);
    Operation op;
    op.client = client;
    op.type = type;
    op.key = std::move(key);
    op.input = std::move(input);
    op.call = ++clock_;
    operations_.push_back(std::move(op));
    cancelled_.push_back(false);
    return operations_.size() - 1;
}

void History::complete(std::size_t id, bool found, std::string output) {
    std::lock_guard lock(mutex_);
    Operation& op = operations_.at(id);
    op.completed = true;
    op.found = found;
    op.output = std::move(output);
    op.ret = ++clock_;
}

void History::cancel(std::size_t id) {
    std::lock_guard lock(mutex_);
    cancelled_.at(id) = true;
}

std::vector<Operation> History::operations() const {
    std::lock_guard lock(mutex_);
    std::vector<Operation> out;
    for (std::size_t i = 0; i < operations_.size(); ++i)
        if (!cancelled_[i]) out.push_back(operations_[i]);
    return out;
}

}  // namespace kv::verify
