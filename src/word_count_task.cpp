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

#include "word_count_task.h"
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <fstream>
#include <map>
#include <iostream>
#include <numeric>

#define TRUNCATION_SIZE 3

// Map interface
std::vector<std::pair<std::string, int>> WordCountTask::map(const std::string& key, const std::string& fileLoc) const {
    std::ifstream f;
    f.open(fileLoc);
    std::map<std::string, int> tokenCounts;
    while (f) {
        std::string sTemp;
        while (sTemp.size() < TRUNCATION_SIZE && f) {
            sTemp += f.get();
        }
        if (sTemp.size() == TRUNCATION_SIZE) {
            if (tokenCounts.contains(sTemp)) {
                ++tokenCounts[sTemp];
            } else {
                tokenCounts[sTemp] = 1;
            }
        }
    }
    f.close();
    return std::vector<std::pair<std::string, int>>(tokenCounts.cbegin(), tokenCounts.cend());
}

// Reduce interface
std::vector<int> WordCountTask::reduce(const std::string& key, const std::vector<int>& counts) const {
    return std::vector<int>(1, std::accumulate(counts.cbegin(), counts.cend(), 0));
}

// Serialize/hash intermediate key functionality (always to and from a string)
// Hash string must map 1-1 with key (no collisions!)
std::string WordCountTask::serializeReduceKey(const std::string& key) const {
    return key;
}

std::string WordCountTask::deserializeReduceKey(const std::string& keyString) const {
    return keyString;
}

std::string WordCountTask::serializeMapKey(const std::string& key) const {
    return key;
}

std::string WordCountTask::deserializeMapKey(const std::string& keyString) const {
    return keyString;
}

// Serialize intermediate value functionality
std::string WordCountTask::serializeReduceValue(const int& value) const {
    return std::to_string(value);
}

int WordCountTask::deserializeReduceValue(const std::string& valueString) const {
    return std::stoi(valueString);
}

std::string WordCountTask::serializeMapValue(const std::string& value) const {
    return value;
}

std::string WordCountTask::deserializeMapValue(const std::string& valueString) const {
    return valueString;
}

const std::map<std::string, std::string>& WordCountTask::getTaskSet() const {
    return taskSet_;
}

const std::string& WordCountTask::getOutFolder() const {
    return outFolder_;
}