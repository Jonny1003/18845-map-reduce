/**
 * @file mini_basic_map_reduce.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief MapReduce using single master
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef MINI_BASIC_MAP_REDUCE_H
#define MINI_BASIC_MAP_REDUCE_H

#include <iostream>
#include <set>
#include <vector>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <type_traits>

#include "map_reduce.h"
#include "util.h"

class MiniBasicMapReduce : MapReduce {
public:
    using MR = MapReduce;

    MiniBasicMapReduce(int argc, char *argv[]) : MR(argc, argv) {
        if (MR::nproc == 1) {
            if (MR::isMaster()) {
                std::cout << "Not enough processors to run basic map reduce!\n";
            }
        }
        numWorkers = nproc - 1;

        // Initialize any other common data here
        if (MR::isMaster()) {
            // Create master coordinator data structures
            progressManager.resize(MR::nproc);
            for (auto& worker : progressManager) {
                worker.type = NONE;
                worker.mapAssigned.clear();
                worker.mapFinished.clear();
                worker.reduceAssigned.clear();
                worker.reduceFinished.clear();
            }
        }
    };

    int keyToPid(const std::string& key) {
        return 1 + (std::hash<std::string>{}(key) % numWorkers);
    };

    template<class Task>
    int run(const Task& task) {
        size_t numMapTasks;
        std::vector<size_t> mapsPerThread;
        if (isMaster()) {
            // initialize coordinator
            const auto& taskSet = task.getTaskSet();

            std::cout << "Total tasks " << taskSet.size() << "\n";
            std::cout << "Number of workers " << numWorkers << "\n";

            // Static work allocation for now...
            // TODO: Modify this to use a more sophisticated work allocation if there is time
            size_t numMapsPerThread = (taskSet.size() + numWorkers - 1) / numWorkers;

            // Assign work to each task
            auto workerIt = progressManager.begin() + 1; // Skip MASTER
            for (auto taskIt = taskSet.cbegin(); taskIt != taskSet.cend(); ++taskIt) {
                workerIt->mapAssigned.insert(task.serializeMapKey(taskIt->first));
                workerIt->type = MAP;
                workerIt->progress = QUEUEING;
                std::advance(workerIt, 1);
                if (workerIt == progressManager.cend()) {
                    workerIt = progressManager.begin() + 1;
                }
            }

            for (const auto& worker : progressManager) {
                mapsPerThread.push_back(worker.mapAssigned.size());
            }
        }

        MPI_Scatter(mapsPerThread.data(), 1, MPI_LONG,
            &numMapTasks, 1, MPI_LONG, MASTER_THREAD, MPI_COMM_WORLD);
        std::cout << MR::pid << " assigned " << numMapTasks << " tasks\n";

        std::map<std::string, std::vector<std::string>> localMapResults;
        if (isMaster()) {
            // Master sends out k->v map tasks to workers
            for (auto wid = 0; wid < progressManager.size(); ++wid) {
                auto& worker = progressManager[wid];
                for (const auto& key : worker.mapAssigned) {
                    const auto& value = task.serializeMapValue(task.getTaskSet().find(key)->second);
                    int kvSizeBuf[2] = {key.length(), value.length()};
                    MPI_Send(kvSizeBuf, 2, MPI_INT, wid, 0, MPI_COMM_WORLD);
                    size_t kvLen = kvSizeBuf[0] + kvSizeBuf[1];
                    char kvBuf[kvLen];
                    size_t ci = 0;
                    for (const auto& c : key) {
                        kvBuf[ci] = c; ++ci;
                    }
                    for (const auto& c : value) {
                        kvBuf[ci] = c; ++ci;
                    }
                    MPI_Send(kvBuf, kvLen, MPI_CHAR, wid, 0, MPI_COMM_WORLD);
                }
                worker.progress = IN_PROGRESS;
            }
        } else {
            std::map<std::string, std::string> localMapWork;
            for (size_t ti = 0; ti < numMapTasks; ++ti) {
                // Get key and value size
                int kvSizeBuf[2];
                MPI_Recv(kvSizeBuf, 2, MPI_INT, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // Get key and value
                size_t kvSize = kvSizeBuf[0] + kvSizeBuf[1];
                char kv[kvSize];
                MPI_Recv(kv, kvSize, MPI_CHAR, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                const auto key = std::string(kv, kvSizeBuf[0]);
                const auto val = std::string(kv + kvSizeBuf[0], kvSizeBuf[1]);
                localMapWork.emplace(std::make_pair(key, std::move(val)));
                // std::cout << "PID: " << MR::pid << " " << localMapWork.find(key)->second << "\n";
            }

            // Run map task for each key
            for (const auto& work : localMapWork) {
                const auto res = task.map(task.deserializeMapKey(work.first), task.deserializeMapValue(work.second));
                for (const auto& [k,v] : res) {
                    if (localMapResults.contains(k)) {
                        localMapResults[k].push_back(task.serializeReduceValue(v));
                    } else {
                        std::vector<std::string> vals;
                        vals.push_back(task.serializeReduceValue(v));
                        localMapResults[k] = std::move(vals);
                    }
                    // std::cout << "PID: " << MR::pid << " " << k << " " << localMapResults.find(k)->second[0] << "\n";
                }
            }
        }

        std::cout << "PID " << MR::pid << " completed map tasks\n";

        // Gather number of reduce keys
        std::vector<size_t> numReduceKeysPerThread(MR::nproc);
        size_t localMapResSize = localMapResults.size();
        MPI_Gather(&localMapResSize, 1, MPI_LONG,
            numReduceKeysPerThread.data(), 1, MPI_LONG,
            MASTER_THREAD, MPI_COMM_WORLD);
        std::map<std::string, std::vector<std::string>> reduceWork;
        if (isMaster()) {
            std::multimap<std::string, std::pair<int,int>> keySources;

            // Gather and sort keys into reduce work
            for (auto srcid = 1; srcid < MR::nproc; ++srcid) {
                // Recieve an array of key sizes
                std::vector<size_t> keyWidths(numReduceKeysPerThread[srcid]);
                MPI_Recv(keyWidths.data(), keyWidths.size(), MPI_LONG, srcid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                size_t totLen = std::accumulate(keyWidths.cbegin(), keyWidths.cend(), 0);
                std::vector<char> keys(totLen);
                MPI_Recv(keys.data(), keys.size(), MPI_CHAR, srcid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Fetch the number of entries associated with each key
                std::vector<int> numEntries(numReduceKeysPerThread[srcid]);
                MPI_Recv(numEntries.data(), numEntries.size(), MPI_INT, srcid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                std::cout << "Master recieved " << totLen << " bytes of keys from PID " << srcid << "\n";

                size_t keyStart = 0;
                for (size_t ki = 0; ki < numReduceKeysPerThread[srcid]; ++ki) {
                    const auto& width = keyWidths[ki];
                    const auto key = std::string(&(keys.data()[keyStart]), width);
                    const auto reduceWorker = keyToPid(key);
                    keySources.insert({key, {srcid, numEntries[ki]}});
                    progressManager[reduceWorker].reduceAssigned.insert(std::move(key));
                    keyStart += width;
                }
            }

            for (size_t destid = 1; destid < MR::nproc; ++destid) {
                auto& worker = progressManager[destid];
                worker.type = REDUCE;
                worker.progress = QUEUEING;
                const auto numKeys = worker.reduceAssigned.size();
                MPI_Send(&numKeys, 1, MPI_LONG, destid, 0, MPI_COMM_WORLD);
                std::for_each(worker.reduceAssigned.cbegin(), worker.reduceAssigned.cend(), [&](const auto& key) {
                    // TODO: Optimize to batch or make asynchronous
                    // Send key size
                    size_t keySizeSrcs[2];
                    keySizeSrcs[0] = key.length();
                    const auto [startSrc, endSrc] = keySources.equal_range(key);
                    keySizeSrcs[1] = std::distance(startSrc, endSrc);
                    const auto& numSrcs = keySizeSrcs[1];
                    MPI_Send(&keySizeSrcs, 2, MPI_LONG, destid, 0, MPI_COMM_WORLD);

                    // Send key
                    MPI_Send(key.data(), key.length(), MPI_CHAR, destid, 0, MPI_COMM_WORLD);

                    // Send key sources
                    std::vector<int> srcs(numSrcs * 2);
                    auto srcIt = startSrc;
                    for (size_t i = 0; i < numSrcs; ++i) {
                        srcs[2 * i] = srcIt->second.first;
                        srcs[2 * i + 1] = srcIt->second.second;
                        std::advance(srcIt, 1);
                    }
                    MPI_Send(srcs.data(), srcs.size(), MPI_INT, destid, 0, MPI_COMM_WORLD);
                });
            }

            std::cout << "Master sent all reduce keys!\n";
        } else {
            // Send reduce keys to master
            std::vector<size_t> keyWidths(localMapResSize);
            std::transform(localMapResults.cbegin(), localMapResults.cend(), keyWidths.begin(),
                [] (const auto& kv) {return kv.first.length();});
            MPI_Send(keyWidths.data(), keyWidths.size(), MPI_LONG, MASTER_THREAD, 0, MPI_COMM_WORLD);
            std::vector<char> keys;
            std::vector<int> numEntries;
            std::for_each(localMapResults.cbegin(), localMapResults.cend(),
                [&keys, &numEntries] (const auto& kv) {
                    keys.insert(keys.cend(), kv.first.cbegin(), kv.first.cend());
                    numEntries.push_back(kv.second.size());});
            MPI_Send(keys.data(), keys.size(), MPI_CHAR, MASTER_THREAD, 0, MPI_COMM_WORLD);
            MPI_Send(numEntries.data(), numEntries.size(), MPI_INT, MASTER_THREAD, 0, MPI_COMM_WORLD);
            std::cout << "PID " << MR::pid << " sent " << keys.size() << " bytes of keys to master\n";

            // Get assigned keys from master
            size_t numKeys;
            MPI_Recv(&numKeys, 1, MPI_LONG, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            std::map<std::string, std::vector<std::pair<int, int>>> keySources;
            for (size_t i = 0; i < numKeys; ++i) {
                size_t keySizeSrcs[2];
                MPI_Recv(keySizeSrcs, 2, MPI_LONG, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                const size_t& keySize = keySizeSrcs[0];
                const size_t& numSrcs = keySizeSrcs[1];

                // Get key
                std::vector<char> key(keySize);
                MPI_Recv(key.data(), key.size(), MPI_CHAR, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                // Get key sources and number of mapped values
                std::vector<int> srcs(numSrcs * 2);
                MPI_Recv(srcs.data(), srcs.size(), MPI_INT, MASTER_THREAD, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                std::vector<std::pair<int, int>> srcPairs;
                for (size_t i = 0; i < numSrcs; ++i) {
                    srcPairs.push_back({srcs[2 * i], srcs[2 * i + 1]});
                }
                // std::cout << "Pid " << MR::pid << " got key " << std::string(key.data(), key.size()) << "\n";
                // for (const auto& [p,n] : srcPairs) {
                //     std::cout << p << " " << n << "\n";
                // }
                keySources[std::string(key.data(), key.size())] = std::move(srcPairs);
            }
            std::cout << "PID " << MR::pid << " recieved " << keySources.size() << " keys from MASTER\n";


            // Fetch values for each key
            size_t numVals = std::accumulate(localMapResults.cbegin(), localMapResults.cend(), 0,
                [] (const size_t& acc, const auto& res) {
                    return acc + res.second.size();});
            std::vector<MPI_Request> requests(numVals * 2);
            std::vector<size_t> valueLens(numVals * 2);
            size_t numReqs = 0;
            for (auto it = localMapResults.cbegin(); it != localMapResults.cend(); ++it) {
                const auto destpid = keyToPid(it->first);
                if (destpid == MR::pid) {
                    if (reduceWork.contains(it->first)) {
                        reduceWork[it->first].insert(reduceWork[it->first].cend(), it->second.cbegin(), it->second.cend());
                    } else {
                        reduceWork[it->first] = it->second;
                    }
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

            // Send reduce sizes values per key
            for (auto keyIt = keySources.cbegin(); keyIt != keySources.cend(); ++keyIt) {
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
            MPI_Waitall(numReqs, requests.data(), MPI_STATUSES_IGNORE);
            std::cout << "PID " << MR::pid << " retrieved all reduce values assigned\n";
        }

        // Do reduce operation
        if (!isMaster()) {
            for (const auto& [k,vs] : reduceWork) {
                std::ofstream out;
                out.open(task.getOutFolder() + "/" + k);
                using VR = decltype(task.deserializeReduceValue(""));
                std::vector<VR> vrs(vs.size());
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
        }

        return 0;
    }

private:
    size_t numWorkers;

    // For MASTER access only
    std::vector<WorkerProgress<std::string, std::string>> progressManager;
};

#endif /* MINI_BASIC_MAP_REDUCE_H */