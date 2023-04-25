/**
 * @file grep.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Simple identity task
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#ifndef GREP_H
#define GREP_H


#include <stdlib.h>
#include <string>
#include <map>
#include <vector>


/**
 * @brief Basic grep function using MapReduce paradigm
 */
class Grep {
public:
    // Provide type defs here
    using MK = std::string;
    using RK = std::string;
    using MV = std::string;
    using RV = std::pair<int, std::string>;
    using TaskType = std::map<MK, MV>;

    Grep(TaskType&& taskSet, std::string& outFolder) :
        taskSet_(std::move(taskSet)), outFolder_(outFolder) {};

    // Map interface
    std::vector<std::pair<RK, RV>> map(const MK& key, const MV& value) const;

    // Reduce interface
    std::vector<RV> reduce(const RK& key, const std::vector<RV>& values) const;

    // Serialize/hash intermediate key functionality (always to and from a string)
    // Hash string must map 1-1 with key (no collisions!)
    std::string serializeReduceKey(const RK& key) const;
    RK deserializeReduceKey(const std::string& keyString) const;

    std::string serializeMapKey(const MK& key) const;
    MK deserializeMapKey(const std::string& keyString) const;

    // Serialize intermediate value functionality
    std::string serializeReduceValue(const RV& value) const;
    RV deserializeReduceValue(const std::string& valueString) const;

    std::string serializeMapValue(const MV& value) const;
    MV deserializeMapValue(const std::string& valueString) const;

    const TaskType& getTaskSet() const;
    const std::string& getOutFolder() const;

private:
    TaskType taskSet_;
    std::string outFolder_;
};

#endif /* GREP_H */