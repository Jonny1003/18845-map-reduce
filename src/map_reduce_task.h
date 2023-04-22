/**
 * @file mr_task.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Abstract class type for structuring a map reduce task
 *          NOTE: This is purely for demonstration purposes and
 *                should NOT be subclassed
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef MAP_REDUCE_TASK_H
#define MAP_REDUCE_TASK_H

#include <stdlib.h>
#include <string.h>
#include <map>
#include <vector>

/**
 * @brief Abstract map reduce class
 *
 * @tparam KM key to map over
 * @tparam VM value to map over
 * @tparam KR key to reduce over
 * @tparam VR value to reduce a list over
 */
template <class KM, class VM, class KR, class VR>
class MapReduceTask {
public:
    // Constructor copies task set into local space
    MapReduceTask(std::map<KM, VM>&& taskSet, std::string& outFolder) :
        taskSet_(std::move(taskSet)), outFolder_(outFolder) {};

    virtual ~MapReduceTask() {};

    // Map interface
    virtual std::vector<std::pair<KR, VR>> map(const KM& key, const VM& value) = 0;

    // Reduce interface
    virtual std::vector<VR> reduce(const KR& key, const std::vector<VR>& values) = 0;

    // Serialize/hash intermediate key functionality (always to and from a string)
    // Hash string must map 1-1 with key (no collisions!)
    virtual std::string serializeReduceKey(const KR& key) = 0;
    virtual KR deserializeReduceKey(const std::string& keyString) = 0;

    // Serialize/hash initial key functionality (always to and from a string)
    // Hash string must map 1-1 with key (no collisions!)
    virtual std::string serializeMapKey(const KM& key) = 0;
    virtual KM deserializeMapKey(const std::string& keyString) = 0;

    // Serialize intermediate value functionality
    virtual std::string serializeReduceValue(const VR& value) = 0;
    virtual VR deserializeReduceValue(const std::string& valueString) = 0;

private:
    std::map<KM, VM> taskSet_;
    std::string outFolder_;
};

#endif /* MAP_REDUCE_TASK_H */