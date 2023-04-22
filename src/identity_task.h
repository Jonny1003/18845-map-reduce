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

#ifndef IDENTITY_TASK_H
#define IDENTITY_TASK_H


#include <stdlib.h>
#include <string>
#include <map>
#include <vector>


/**
 * @brief Identity task
 */
class IdentityTask {
public:
    IdentityTask(std::map<std::string, int>&& taskSet, std::string& outFolder) :
        taskSet_(std::move(taskSet)), outFolder_(outFolder) {};

    // Map interface
    std::vector<std::pair<std::string, int>> map(const std::string& key, const int& value) const;

    // Reduce interface
    std::vector<int> reduce(const std::string& key, const std::vector<int>& values) const;

    // Serialize/hash intermediate key functionality (always to and from a string)
    // Hash string must map 1-1 with key (no collisions!)
    std::string serializeReduceKey(const std::string& key) const;
    std::string deserializeReduceKey(const std::string& keyString) const;

    std::string serializeMapKey(const std::string& key) const;
    std::string deserializeMapKey(const std::string& keyString) const;

    // Serialize intermediate value functionality
    std::string serializeReduceValue(const int& value) const;
    int deserializeReduceValue(const std::string& valueString) const;

    std::string serializeMapValue(const int& value) const;
    int deserializeMapValue(const std::string& valueString) const;

    const std::map<std::string, int>& getTaskSet() const;

private:
    std::map<std::string, int> taskSet_;
    std::string outFolder_;
};

#endif /* IDENTITY_TASK_H */