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
#include "map_reduce.h"
#include <set>
#include <vector>
#include <algorithm>
#include <numeric>

enum WorkerType {MAP, REDUCE, NONE};
enum Progress {QUEUEING, IN_PROGRESS, COMPLETED};

// Per worker state manager
template<class KM, class KR>
struct WorkerProgress {
    WorkerType type;
    Progress progress;
    std::set<KM> mapAssigned;
    std::set<KR> reduceAssigned;
    // Below are not used at the moment
    // since all communication is synchronous for now
    std::set<KM> mapFinished;
    std::set<KR> reduceFinished;
};

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

    template<class Task>
    int run(const Task& task) {
        size_t numMapTasks;
        std::vector<size_t> mapsPerThread;
        if (isMaster()) {
            // initialize coordinator
            const auto& taskSet = task.getTaskSet();

            // MPI_Bcast(&numMapTasks, 1, MPI_LONG, MASTER_THREAD, MPI_COMM_WORLD);
            std::cout << "Total tasks" << taskSet.size() << "\n";

            // Static work allocation for now...
            // TODO: Modify this to use a more sophisticated work allocation if there is time
            size_t numMapsPerThread = (taskSet.size() + numWorkers - 1) / numWorkers;

            // Assign work to each task
            auto workToAlloc = numMapsPerThread;
            auto workerIt = progressManager.begin() + 1; // Skip MASTER
            for (auto taskIt = taskSet.cbegin(); taskIt != taskSet.cend(); ++taskIt) {
                if (workToAlloc == 0) {
                    // Reset and move to next worker
                    workToAlloc = numMapsPerThread;
                    std::advance(workerIt, 1);
                }
                workerIt->mapAssigned.insert(task.serializeMapKey(taskIt->first));
                workerIt->type = MAP;
                workerIt->progress = QUEUEING;
                --workToAlloc;
            }

            for (const auto& worker : progressManager) {
                mapsPerThread.push_back(worker.mapAssigned.size());
            }
        }

        MPI_Scatter(mapsPerThread.data(), 1, MPI_LONG,
            &numMapTasks, 1, MPI_LONG, MASTER_THREAD, MPI_COMM_WORLD);
        std::cout << MR::pid << " " << numMapTasks << "\n";

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
                std::cout << "PID: " << MR::pid << " " << localMapWork.find(key)->second << "\n";
            }

            // Run map task for each key
            for (const auto& work : localMapWork) {
                const auto res = task.map(task.deserializeMapKey(work.first), task.deserializeMapValue(work.second));
                for (const auto& [k,v] : res) {
                    if (localMapResults.contains(k)) {
                        localMapResults.find(k)->second.push_back(task.serializeReduceValue(v));
                    } else {
                        std::vector<std::string> vals;
                        vals.push_back(task.serializeReduceValue(v));
                        localMapResults.emplace(task.serializeReduceKey(k), std::move(vals));
                    }
                    std::cout << "PID: " << MR::pid << " " << localMapResults.find(k)->second[0] << "\n";
                }
            }
        }

        // Gather number of reduce keys
        std::vector<size_t> numReduceKeysPerThread(MR::nproc);
        size_t localMapResSize = localMapResults.size();
        MPI_Gather(&localMapResSize, 1, MPI_LONG,
            numReduceKeysPerThread.data(), 1, MPI_LONG,
            MASTER_THREAD, MPI_COMM_WORLD);
        if (isMaster()) {
            // Gather and sort keys into reduce work
            for (auto srcid = 1; srcid < MR::nproc; ++srcid) {
                // Recieve an array of key sizes
                std::vector<size_t> keyWidths(numReduceKeysPerThread[srcid]);
                MPI_Recv(keyWidths.data(), keyWidths.size(), MPI_LONG, srcid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                size_t totLen = std::accumulate(keyWidths.cbegin(), keyWidths.cend(), 0);
                char keys[totLen];
                MPI_Recv(keys, totLen, MPI_CHAR, srcid, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                size_t keyStart = 0;
                for (const auto& width : keyWidths) {
                    const auto key = std::string(&keys[keyStart], width);
                    std::cout << key << "\n";
                    const auto reduceWorker = 1 + (std::hash<std::string>{}(key) % numWorkers);
                    std::cout << "MASTER: " << key << "->" << reduceWorker << "\n";
                    progressManager[reduceWorker].reduceAssigned.insert(std::move(key));
                    keyStart += width;
                }
            }
        } else {
            // Send reduce keys to master
            std::vector<size_t> keyWidths(localMapResSize);
            std::transform(localMapResults.cbegin(), localMapResults.cend(), keyWidths.begin(),
                [] (const auto& kv) {return kv.first.length();});
            MPI_Send(keyWidths.data(), keyWidths.size(), MPI_LONG, MASTER_THREAD, 0, MPI_COMM_WORLD);
            std::vector<char> keys;
            std::for_each(localMapResults.cbegin(), localMapResults.cend(),
                [&keys] (const auto& kv) {keys.insert(keys.cend(), kv.first.cbegin(), kv.first.cend());});
            MPI_Send(keys.data(), keys.size(), MPI_CHAR, MASTER_THREAD, 0, MPI_COMM_WORLD);
        }

        // Send reduce keys
        


        return 0;
    }

private:
    size_t numWorkers;

    // For MASTER access only
    std::vector<WorkerProgress<std::string, std::string>> progressManager;
};

#endif /* MINI_BASIC_MAP_REDUCE_H */