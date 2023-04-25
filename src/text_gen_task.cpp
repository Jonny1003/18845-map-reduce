/**
 * @file identity_task.c
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Generates large blocks of text
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include "text_gen_task.h"
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <fstream>
#include <ctime>




constexpr auto UNEVEN_DISTRIBUTION = false;

// Set to some reasonable value less than system ram
constexpr size_t TOT_NUM_BYTES = 2 * (1UL << 29);

// Map interface
std::vector<std::pair<std::string, std::string>> TextGenTask::map(const std::string& key, const std::string& value) const {
    (void) value;
    std::srand(std::time(0));
    auto bytesPerTask = TOT_NUM_BYTES / taskSet_.size();
    if constexpr (UNEVEN_DISTRIBUTION) {
        const auto ratio = 2.0f * static_cast<float>(std::rand()) / static_cast<float>(RAND_MAX);
        bytesPerTask = static_cast<size_t>(ratio * bytesPerTask);
    }

    std::string s;
    for (size_t i = 0; i < bytesPerTask; ++i) {
        s += (std::rand() % (123 - 97)) + 97;
    }

    return std::vector<std::pair<std::string, std::string>>({{key, s}});
}

// Reduce interface
std::vector<std::string> TextGenTask::reduce(const std::string& key, const std::vector<std::string>& values) const {
    return values;
}

// Serialize/hash intermediate key functionality (always to and from a string)
// Hash string must map 1-1 with key (no collisions!)
std::string TextGenTask::serializeReduceKey(const std::string& key) const {
    return key;
}

std::string TextGenTask::deserializeReduceKey(const std::string& keyString) const {
    return keyString;
}

std::string TextGenTask::serializeMapKey(const std::string& key) const {
    return key;
}

std::string TextGenTask::deserializeMapKey(const std::string& keyString) const {
    return keyString;
}

// Serialize intermediate value functionality
std::string TextGenTask::serializeReduceValue(const std::string& value) const {
    return value;
}

std::string TextGenTask::deserializeReduceValue(const std::string& valueString) const {
    return valueString;
}

std::string TextGenTask::serializeMapValue(const std::string& value) const {
    return value;
}

std::string TextGenTask::deserializeMapValue(const std::string& valueString) const {
    return valueString;
}

const std::map<std::string, std::string>& TextGenTask::getTaskSet() const {
    return taskSet_;
}

const std::string& TextGenTask::getOutFolder() const {
    return outFolder_;
}