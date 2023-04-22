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

#include "doubling_task.h"
#include <vector>
#include <string>

// Map interface
std::vector<std::pair<std::string, int>> DoublingTask::map(const std::string& key, const int& value) const {
    return std::vector<std::pair<std::string, int>>({{key, value}, {key,value}, {key + "x",value}, {key + "y",value}});
}

// Reduce interface
std::vector<int> DoublingTask::reduce(const std::string& key, const std::vector<int>& values) const {
    (void) key;
    int acc = 0;
    for (const auto& v : values) {
        acc += v;
    }
    return std::vector<int>({acc});
}

// Serialize/hash intermediate key functionality (always to and from a string)
// Hash string must map 1-1 with key (no collisions!)
std::string DoublingTask::serializeReduceKey(const std::string& key) const {
    return key;
}

std::string DoublingTask::deserializeReduceKey(const std::string& keyString) const {
    return keyString;
}

std::string DoublingTask::serializeMapKey(const std::string& key) const {
    return key;
}

std::string DoublingTask::deserializeMapKey(const std::string& keyString) const {
    return keyString;
}

// Serialize intermediate value functionality
std::string DoublingTask::serializeReduceValue(const int& value) const {
    return std::to_string(value);
}

int DoublingTask::deserializeReduceValue(const std::string& valueString) const {
    return stoi(valueString);
}

std::string DoublingTask::serializeMapValue(const int& value) const {
    return std::to_string(value);
}

int DoublingTask::deserializeMapValue(const std::string& valueString) const {
    return stoi(valueString);
}

const std::map<std::string, int>& DoublingTask::getTaskSet() const {
    return taskSet_;
}

const std::string& DoublingTask::getOutFolder() const {
    return outFolder_;
}