/**
 * @file identity_task.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Simple identity task
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#ifndef TEXT_GEN_TASK_H
#define TEXT_GEN_TASK_H


#include <stdlib.h>
#include <string>
#include <map>
#include <vector>
#include <iostream>



/**
 * @brief Generates ASCII text task
 */
class TextGenTask {
public:
    using MK = std::string;
    using RK = std::string;
    using MV = std::string;
    using RV = std::string;
    using TaskType = std::map<std::string, std::string>;

    TextGenTask(std::map<std::string, std::string>&& taskSet, std::string& outFolder) :
        taskSet_(std::move(taskSet)), outFolder_(outFolder) {};

    // Map interface
    std::vector<std::pair<std::string, std::string>> map(const std::string& key, const std::string& value) const;

    // Reduce interface
    std::vector<std::string> reduce(const std::string& key, const std::vector<std::string>& values) const;

    // Serialize/hash intermediate key functionality (always to and from a string)
    // Hash string must map 1-1 with key (no collisions!)
    std::string serializeReduceKey(const std::string& key) const;
    std::string deserializeReduceKey(const std::string& keyString) const;

    std::string serializeMapKey(const std::string& key) const;
    std::string deserializeMapKey(const std::string& keyString) const;

    // Serialize intermediate value functionality
    std::string serializeReduceValue(const std::string& value) const;
    std::string deserializeReduceValue(const std::string& valueString) const;

    std::string serializeMapValue(const std::string& value) const;
    std::string deserializeMapValue(const std::string& valueString) const;

    const std::map<std::string, std::string>& getTaskSet() const;
    const std::string& getOutFolder() const;

private:
    std::map<std::string, std::string> taskSet_;
    std::string outFolder_;
};

#endif /* TEXT_GEN_TASK_H */