/**
 * @file mini_basic_map_reduce.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief MapReduce using fat tree communication structure
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */


#ifndef MINI_TREE_MAP_REDUCE
#define MINI_TREE_MAP_REDUCE

#include <iostream>
#include <set>
#include <vector>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <type_traits>
#include <chrono>
#include <functional>

#include "map_reduce.h"
#include "util.h"

class MiniTreeMapReduce : public MapReduce {
public:
    using MR = MapReduce;

    MiniTreeMapReduce(int argc, char *argv[]) : MR(argc, argv) {
        if (MR::nproc == 1) {
            if (MR::isMaster()) {
                std::cout << "Not enough processors to run basic map reduce!\n";
            }
        }

        std::function<void(int, std::set<int>&)> f;
        f = [&] (int pid, std::set<int>& childPids) {
            if (pid >= 0 && pid < MR::nproc) {
                childPids.insert(pid);
            } else {
                return;
            }
            f(2 * (pid + 1) - 1, childPids);
            f(2 * (pid + 1), childPids);
        };
        const auto child1 = 2 * (MR::pid + 1) - 1;
        if (child1 < MR::nproc) {
            childrenPids.push_back(child1);
            f(child1, child1PidGroup);
        }
        const auto child2 = child1 + 1;
        if (child2 < MR::nproc) {
            childrenPids.push_back(child2);
            f(child2, child2PidGroup);
        }
        parent = (MR::pid + 1) / 2 - 1;
        numWorkers = 1 + childrenPids.size();

        // Initialize any other common data here
        // Create master coordinator data structures
        progressManager.resize(numWorkers);
        for (auto& worker : progressManager) {
            worker.type = NONE;
            worker.mapAssigned.clear();
            worker.mapFinished.clear();
            worker.reduceAssigned.clear();
            worker.reduceFinished.clear();
        }
    };

    int keyToPid(const std::string& key) {
        return std::hash<std::string>{}(key) % MR::nproc;
    };

    template<class Task>
    int run(const Task& task) {
        std::vector<std::chrono::microseconds> timings;

        auto initTime = std::chrono::high_resolution_clock::now();

        size_t numMapsPerThread;
        if (isMaster()) {
            numMapsPerThread = (task.getTaskSet().size() + MR::nproc - 1) / MR::nproc;
        }
        MPI_Bcast(&numMapsPerThread, 1, MPI_LONG, MASTER_THREAD, MPI_COMM_WORLD);

        size_t numMapTasks;
        std::map<std::string, std::string> localMapWork;
        if (isMaster()) {
            // initialize coordinator
            const auto& taskSet = task.getTaskSet();
            std::cout << "Total tasks " << taskSet.size() << "\n";

            // Static work allocation for now...
            size_t workAssignedToChild1 = (taskSet.size() - numMapsPerThread) / childrenPids.size();

            // Assign work to each task
            auto workerIt = progressManager.begin();
            auto workCurrAssignedToMe = 0;
            auto workCurrAssignedToChild1 = 0;
            for (auto taskIt = taskSet.cbegin(); taskIt != taskSet.cend(); ++taskIt) {
                std::string key = task.serializeMapKey(taskIt->first);
                if (workCurrAssignedToMe < numMapsPerThread) {
                    workCurrAssignedToMe++;
                } else if (workCurrAssignedToChild1 < workAssignedToChild1) {
                    workCurrAssignedToChild1++;
                }
                // Add to local work queue
                localMapWork.emplace(std::make_pair(key,
                    task.serializeMapValue(taskSet.find(key)->second)));
                workerIt->mapAssigned.insert(std::move(key));
                workerIt->type = MAP;
                workerIt->progress = QUEUEING;
                if (workCurrAssignedToMe == numMapsPerThread) {
                    workCurrAssignedToMe++; // Disable
                    std::advance(workerIt, 1);
                } else if (workCurrAssignedToChild1 == workAssignedToChild1) {
                    workCurrAssignedToChild1++; // Disable
                    std::advance(workerIt, 1);
                }
            }
            std::cout << "Master tasks assigned\n";
        } else {
            std::cout << "Pid " << MR::pid << " waiting for map keys from " << parent << "\n";
            // Get keys and values from parent
            MPI_Recv(&numMapTasks, 1, MPI_LONG, parent, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Work distribution functions
            if (childrenPids.size() == 0) {
                numMapsPerThread = numMapTasks;
            }
            size_t workAssignedToChild1 = childrenPids.size() ?
                std::max((numMapTasks - numMapsPerThread) / childrenPids.size(), 0UL) : 0UL;
            auto workCurrAssignedToMe = 0; auto workCurrAssignedToChild1 = 0;
            std::cout << MR::pid << " has work distribution " << numMapsPerThread << " and "<< workAssignedToChild1 << " and " << numMapTasks << "\n";

            for (size_t ti = 0; ti < numMapTasks; ++ti) {
                // Get key and value size
                int kvSizeBuf[2];
                MPI_Recv(kvSizeBuf, 2, MPI_INT, parent, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // Get key and value
                size_t kvSize = kvSizeBuf[0] + kvSizeBuf[1];
                char kv[kvSize];
                MPI_Recv(kv, kvSize, MPI_CHAR, parent, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                const auto key = std::string(kv, kvSizeBuf[0]);
                const auto val = std::string(kv + kvSizeBuf[0], kvSizeBuf[1]);

                // Partition work to children
                localMapWork.emplace(std::make_pair(key, std::move(val)));
                auto idxAssigned = -1;
                if (workCurrAssignedToMe < numMapsPerThread) {
                    ++workCurrAssignedToMe;
                    idxAssigned = 0;
                } else if (workCurrAssignedToChild1 < workAssignedToChild1) {
                    ++workCurrAssignedToChild1;
                    idxAssigned = 1;
                } else {
                    idxAssigned = 2;
                }
                progressManager[idxAssigned].mapAssigned.insert(std::move(key));
            }
            std::cout << MR::pid << " recieved map keys from parent " << parent << "\n";
        }

        // Send keys to children
        std::cout << MR::pid << " sending keys to children\n";
        for (auto idx = 1; idx < progressManager.size(); ++idx) {
            size_t childMapTasks = progressManager[idx].mapAssigned.size();
            const auto& childPid = childrenPids[idx-1];
            std::cout << MR::pid << " sending to " << childPid << "\n";
            MPI_Send(&childMapTasks, 1, MPI_LONG, childPid, 0, MPI_COMM_WORLD);

            // Master sends out k->v map tasks to child workers
            auto& worker = progressManager[idx];
            for (const auto& key : worker.mapAssigned) {
                const auto& value = localMapWork.find(key)->second;
                int kvSizeBuf[2] = {key.length(), value.length()};
                MPI_Send(kvSizeBuf, 2, MPI_INT, childPid, 0, MPI_COMM_WORLD);
                size_t kvLen = kvSizeBuf[0] + kvSizeBuf[1];
                char kvBuf[kvLen];
                size_t ci = 0;
                for (const auto& c : key) {
                    kvBuf[ci] = c; ++ci;
                }
                for (const auto& c : value) {
                    kvBuf[ci] = c; ++ci;
                }
                MPI_Send(kvBuf, kvLen, MPI_CHAR, childPid, 0, MPI_COMM_WORLD);
                localMapWork.erase(key);
            }
            worker.progress = IN_PROGRESS;
        }
        timings.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - initTime));
        initTime = std::chrono::high_resolution_clock::now();

        // Run map task for each key
        std::map<std::string, std::vector<std::string>> localMapResults;
        for (const auto& work : localMapWork) {
            const auto res = task.map(task.deserializeMapKey(work.first), task.deserializeMapValue(work.second));
            for (const auto& [k,v] : res) {
                if (localMapResults.find(k) != localMapResults.cend()) {
                    localMapResults[k].push_back(task.serializeReduceValue(v));
                } else {
                    std::vector<std::string> vals;
                    vals.push_back(task.serializeReduceValue(v));
                    localMapResults[k] = std::move(vals);
                }
            }
        }

        std::cout << "PID " << MR::pid << " completed map tasks\n";
        timings.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - initTime));
        initTime = std::chrono::high_resolution_clock::now();

        std::map<std::string, std::vector<std::pair<int, int>>> keySources;
        auto updateKeySources = [&] (int otherPid) {
            size_t numChildReduceKeys;
            MPI_Recv(&numChildReduceKeys, 1, MPI_LONG, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Recieve an array of key sizes
            std::vector<size_t> keyWidths(numChildReduceKeys);
            MPI_Recv(keyWidths.data(), keyWidths.size(), MPI_LONG, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            size_t totLen = std::accumulate(keyWidths.cbegin(), keyWidths.cend(), 0);
            std::vector<char> keys(totLen);
            MPI_Recv(keys.data(), keys.size(), MPI_CHAR, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Fetch the number of sources associated with each key
            std::vector<int> numEntries(numChildReduceKeys);
            MPI_Recv(numEntries.data(), numEntries.size(), MPI_INT, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            size_t totSrcs = std::accumulate(numEntries.cbegin(), numEntries.cend(), 0);

            // Fetch source to contact for values
            std::vector<int> srcs(totSrcs);
            MPI_Recv(srcs.data(), srcs.size(), MPI_INT, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Fetch entries per source
            std::vector<int> entriesPerSrc(totSrcs);
            MPI_Recv(entriesPerSrc.data(), entriesPerSrc.size(), MPI_INT, otherPid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            std::cout << "PID " << MR::pid << " recieved " << totLen << " bytes of keys from PID " << otherPid << "\n";

            size_t keyStart = 0;
            size_t srcIdx = 0;
            for (size_t ki = 0; ki < numChildReduceKeys; ++ki) {
                const auto& width = keyWidths[ki];
                const auto key = std::string(&(keys.data()[keyStart]), width);
                auto it = keySources.find(key);
                if (it == keySources.cend()) {
                    keySources.emplace(std::move(key), std::vector<std::pair<int, int>>());
                    it = keySources.find(key);
                }
                for (size_t i = srcIdx; i < srcIdx + numEntries[ki]; ++i) {
                    it->second.push_back(std::make_pair(srcs[i], entriesPerSrc[i]));
                }
                srcIdx += numEntries[ki];
                keyStart += width;
            }
        };

        // Gather reduce keys from children
        for (const auto& child : childrenPids) {
            updateKeySources(child);
        }

        std::for_each(localMapResults.cbegin(), localMapResults.cend(),
            [&] (const auto& kv) {
                auto it = keySources.find(kv.first);
                if (it == keySources.cend()) {
                    keySources.emplace(kv.first, std::vector({std::make_pair(MR::pid, (int) kv.second.size())}));
                } else {
                    it->second.push_back(std::make_pair(MR::pid, (int) kv.second.size()));
                }});

        if (!isMaster()) {
            // Send keys to parent
            std::vector<size_t> keyWidths;
            std::vector<char> keys;
            std::vector<int> numEntries;
            std::vector<int> srcs;
            std::vector<int> entriesPerSrc;
            std::for_each(keySources.cbegin(), keySources.cend(),
                [&] (const auto& kvec) {
                    const auto pid = keyToPid(kvec.first);
                    if (pid != MR::pid &&
                        child1PidGroup.find(pid) == child1PidGroup.cend() &&
                        child2PidGroup.find(pid) == child2PidGroup.cend())
                    {
                        keyWidths.push_back(kvec.first.length());
                        keys.insert(keys.cend(), kvec.first.cbegin(), kvec.first.cend());
                        numEntries.push_back(kvec.second.size());
                        for (const auto& [src, entry] : kvec.second) {
                            srcs.push_back(src);
                            entriesPerSrc.push_back(entry);
                        }
                    }});
            size_t numKeys = keyWidths.size();
            MPI_Send(&numKeys, 1, MPI_LONG, parent, 0, MPI_COMM_WORLD);
            MPI_Send(keyWidths.data(), keyWidths.size(), MPI_LONG, parent, 0, MPI_COMM_WORLD);
            MPI_Send(keys.data(), keys.size(), MPI_CHAR, parent, 0, MPI_COMM_WORLD);
            MPI_Send(numEntries.data(), numEntries.size(), MPI_INT, parent, 0, MPI_COMM_WORLD);
            MPI_Send(srcs.data(), srcs.size(), MPI_INT, parent, 0, MPI_COMM_WORLD);
            MPI_Send(entriesPerSrc.data(), entriesPerSrc.size(), MPI_INT, parent, 0, MPI_COMM_WORLD);
            std::cout << "PID " << MR::pid << " sent " << keys.size() << " bytes of keys to parent\n";
        }

        if (!isMaster()) {
            // Get new key values from parent
            updateKeySources(parent);
        }

        // Send keys to children
        for (auto i = 0; i < childrenPids.size(); ++i) {
            const auto child = childrenPids[i];
            // Send keys to child
            std::vector<size_t> keyWidths;
            std::vector<char> keys;
            std::vector<int> numEntries;
            std::vector<int> srcs;
            std::vector<int> entriesPerSrc;
            std::for_each(keySources.cbegin(), keySources.cend(),
                [&] (const auto& kvec) {
                    const auto pid = keyToPid(kvec.first);
                    if ((i == 0 && child1PidGroup.find(pid) != child1PidGroup.cend()) ||
                        (i == 1 && child2PidGroup.find(pid) != child2PidGroup.cend()))
                    {
                        keyWidths.push_back(kvec.first.length());
                        keys.insert(keys.cend(), kvec.first.cbegin(), kvec.first.cend());
                        numEntries.push_back(kvec.second.size());
                        for (const auto& [src, entry] : kvec.second) {
                            srcs.push_back(src);
                            entriesPerSrc.push_back(entry);
                        }
                    }});
            size_t numKeys = keyWidths.size();
            std::cout << "Created data\n";
            MPI_Send(&numKeys, 1, MPI_LONG, child, 0, MPI_COMM_WORLD);
            MPI_Send(keyWidths.data(), keyWidths.size(), MPI_LONG, child, 0, MPI_COMM_WORLD);
            MPI_Send(keys.data(), keys.size(), MPI_CHAR, child, 0, MPI_COMM_WORLD);
            MPI_Send(numEntries.data(), numEntries.size(), MPI_INT, child, 0, MPI_COMM_WORLD);
            MPI_Send(srcs.data(), srcs.size(), MPI_INT, child, 0, MPI_COMM_WORLD);
            MPI_Send(entriesPerSrc.data(), entriesPerSrc.size(), MPI_INT, child, 0, MPI_COMM_WORLD);
            std::cout << "PID " << MR::pid << " sent " << keys.size() << " bytes of keys to " << child << "\n";
        }

        // Fetch values for each key
        std::map<std::string, std::vector<std::string>> reduceWork;
        size_t numVals = std::accumulate(localMapResults.cbegin(), localMapResults.cend(), 0,
            [] (const size_t& acc, const auto& res) {
                return acc + res.second.size();});
        std::vector<MPI_Request> requests(numVals * 2);
        std::vector<size_t> valueLens(numVals * 2);
        size_t numReqs = 0;
        for (auto it = localMapResults.cbegin(); it != localMapResults.cend(); ++it) {
            const auto destpid = keyToPid(it->first);
            if (destpid == MR::pid) {
                // Don't send messages
            } else {
                for (auto val = it->second.cbegin(); val < it->second.cend(); ++val) {
                    // std::cout << *val << "\n";
                    valueLens[numReqs] = val->size();
                    MPI_Isend(&valueLens[numReqs], 1, MPI_LONG, destpid, 0, MPI_COMM_WORLD, &requests[numReqs]);
                    MPI_Isend(val->data(), val->size(), MPI_CHAR, destpid, 0, MPI_COMM_WORLD, &requests[numReqs + 1]);
                    numReqs += 2;
                }
            }
        }
        std::cout << "Pid " << MR::pid << " sending " << numReqs << " requests\n";

        // Get reduce values
        for (auto keyIt = keySources.cbegin(); keyIt != keySources.cend(); ++keyIt) {
            if (keyToPid(keyIt->first) == MR::pid) {
                auto& values = reduceWork.emplace(keyIt->first, std::vector<std::string>()).first->second;
                for (auto srcIt = keyIt->second.cbegin(); srcIt < keyIt->second.cend(); ++srcIt) {
                    if (srcIt->first == MR::pid) {continue;}
                    for (auto valInd = 0; valInd < srcIt->second; ++valInd) {
                        size_t valSize;
                        MPI_Recv(&valSize, 1, MPI_LONG, srcIt->first, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        values.push_back(std::string(valSize, '\0'));
                        MPI_Recv(values[values.size()-1].data(), valSize, MPI_CHAR, srcIt->first, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    }
                }
            }
        }
        MPI_Waitall(numReqs, requests.data(), MPI_STATUSES_IGNORE);
        std::cout << "PID " << MR::pid << " retrieved all reduce values assigned\n";

        timings.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - initTime));
        initTime = std::chrono::high_resolution_clock::now();

        // Do reduce operation
        for (const auto& [k,vs] : reduceWork) {
            std::ofstream out;
            out.open(task.getOutFolder() + "/" + k);
            std::vector<typename Task::RV> vrs(vs.size());
            std::transform(vs.cbegin(), vs.cend(), vrs.begin(), [&task](const auto& v) {
                return task.deserializeReduceValue(v);});
            const auto res = task.reduce(task.deserializeReduceKey(k), vrs);
            for (const auto& r : res) {
                const auto rStr = task.serializeReduceValue(r);
                out << rStr.length() << " " << rStr;
            }
            out.close();
        }
        std::cout << "PID " << MR::pid << " completed reductions!\n";
        timings.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::high_resolution_clock::now() - initTime));
        initTime = std::chrono::high_resolution_clock::now();

        // Send all timings to master
        if (isMaster()) {
            std::ofstream log;
            log.open("./timing-log");
            std::string mSetup = " Map Setup: ";
            std::string mapTime = "us Map Time: ";
            std::string rSetup = "us Reduce Setup: ";
            std::string redTime = "us Reduce Time: ";

            log << "PID " << MR::pid <<
                mSetup << timings[0].count() <<
                mapTime << timings[1].count() <<
                rSetup << timings[2].count() <<
                redTime << timings[3].count() << "us\n";
            for (auto i = 1; i < MR::nproc; ++i) {
                size_t otherTimings[4];
                for (auto j = 0; j < timings.size(); ++j) {
                    MPI_Recv(otherTimings + j, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                }
                log << "PID " << i <<
                    mSetup << otherTimings[0] <<
                    mapTime << otherTimings[1] <<
                    rSetup << otherTimings[2] <<
                    redTime << otherTimings[3] << "us\n";
            }
            log.close();
        } else {
            for (auto j = 0; j < timings.size(); ++j) {
                size_t time = timings[j].count();
                MPI_Send(&time, 1, MPI_LONG, MASTER_THREAD, 0, MPI_COMM_WORLD);
            }
        }

        return 0;
    }

private:
    std::vector<int> childrenPids;
    std::set<int> child1PidGroup;
    std::set<int> child2PidGroup;
    size_t parent;
    int numWorkers;

    // For MASTER access only
    std::vector<WorkerProgress<std::string, std::string>> progressManager;
};

#endif /* MINI_TREE_MAP_REDUCE */