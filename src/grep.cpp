/**
 * @file grep.cpp
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Generates large blocks of text
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include "grep.h"
#include <vector>
#include <string>
#include <random>
#include <functional>
#include <fstream>
#include <iostream>

// Some constants to grep how many characters before and how many after
constexpr auto CONTEXT_BEFORE = 50;
constexpr auto CONTEXT_AFTER = 50;

// Map interface
std::vector<std::pair<Grep::RK, Grep::RV>> Grep::map(const std::string& key, const std::string& value) const {
    std::ifstream f;
    f.open(key);
    std::vector<std::pair<RK, RV>> out;
    const auto keyName = key.substr(key.find_last_of('/') + 1);
    if (f) {
        f.seekg(0, f.end);
        const size_t textSize = f.tellg();
        f.seekg (0, f.beg);
        std::cout << "Worker reading in text size: " << textSize << "\n";

        char *text = new char[textSize];
        f.read(text, textSize);

        for (auto i = 0; i < textSize; ++i) {
            std::string s(text + i, std::min(value.length(), textSize - i));
            if (!s.compare(value)) {
                const auto textMin = std::max(0, i - CONTEXT_BEFORE);
                const auto textMax = std::min((size_t) i + CONTEXT_AFTER, textSize);
                out.push_back({keyName, {i, std::string(text + textMin, textMax - textMin)}});
            }
        }
    }
    f.close();
    return out;
}

// Reduce interface
std::vector<Grep::RV> Grep::reduce(const Grep::RK& key, const std::vector<Grep::RV>& values) const {
    return values;
}

// Serialize/hash intermediate key functionality (always to and from a string)
// Hash string must map 1-1 with key (no collisions!)
std::string Grep::serializeReduceKey(const Grep::RK& key) const {
    return key;
}

Grep::RK Grep::deserializeReduceKey(const std::string& keyString) const {
    return keyString;
}

std::string Grep::serializeMapKey(const Grep::MK& key) const {
    return key;
}

Grep::MK Grep::deserializeMapKey(const std::string& keyString) const {
    return keyString;
}

// Serialize intermediate value functionality
std::string Grep::serializeReduceValue(const Grep::RV& value) const {
    return std::to_string(value.first) + " " + value.second;
}

Grep::RV Grep::deserializeReduceValue(const std::string& valueString) const {
    size_t nextIdx;
    const auto indexNum = std::stoi(valueString, &nextIdx);
    return std::make_pair(indexNum, valueString.substr(nextIdx + 1));
}

std::string Grep::serializeMapValue(const MV& value) const {
    return value;
}

Grep::MV Grep::deserializeMapValue(const std::string& valueString) const {
    return valueString;
}

const std::map<std::string, std::string>& Grep::getTaskSet() const {
    return taskSet_;
}

const std::string& Grep::getOutFolder() const {
    return outFolder_;
}