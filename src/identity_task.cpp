/**
 * @file identity_task.c
 * @author your name (you@domain.com)
 * @brief Identity map reduce task
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include "identity_task.h"
#include <vector>
#include <string>

// Map interface
std::vector<std::pair<std::string, int>> IdentityTask::map(const std::string& key, const int& value) const {
    return std::vector<std::pair<std::string, int>>({{key, value}});
}

// Reduce interface
std::vector<int> IdentityTask::reduce(const std::string& key, const std::vector<int>& values) const {
    (void) key;
    int acc = 0;
    for (const auto& v : values) {
        acc += v;
    }
    return std::vector<int>({acc});
}

// Serialize/hash intermediate key functionality (always to and from a string)
// Hash string must map 1-1 with key (no collisions!)
std::string IdentityTask::serializeReduceKey(const std::string& key) const {
    return key;
}

std::string IdentityTask::deserializeReduceKey(const std::string& keyString) const {
    return keyString;
}

std::string IdentityTask::serializeMapKey(const std::string& key) const {
    return key;
}

std::string IdentityTask::deserializeMapKey(const std::string& keyString) const {
    return keyString;
}

// Serialize intermediate value functionality
std::string IdentityTask::serializeReduceValue(const int& value) const {
    return std::to_string(value);
}

int IdentityTask::deserializeReduceValue(const std::string& valueString) const {
    return stoi(valueString);
}

std::string IdentityTask::serializeMapValue(const int& value) const {
    return std::to_string(value);
}

int IdentityTask::deserializeMapValue(const std::string& valueString) const {
    return stoi(valueString);
}

const std::map<std::string, int>& IdentityTask::getTaskSet() const {
    return taskSet_;
}

const std::string& IdentityTask::getOutFolder() const {
    return outFolder_;
}