/**
 * @file map_reduce.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief MapReduce implementation interface
 * @version 0.1
 * @date 2023-04-20
 *
 * @copyright Copyright (c) 2023
 *
 */

#ifndef MAP_REDUCE_H
#define MAP_REDUCE_H

#include <stdlib.h>
#include "map_reduce_task.h"
#include "mpi.h"

constexpr auto MASTER_THREAD = 0;

class MapReduce {
public:
    MapReduce(int argc, char *argv[]) {
        // Setup MPI
        MPI_Init(&argc, &argv);

        // Get process rank
        MPI_Comm_rank(MPI_COMM_WORLD, &pid);

        // Get total number of processes specificed at start of run
        MPI_Comm_size(MPI_COMM_WORLD, &nproc);
    };

    ~MapReduce() {
        MPI_Finalize();
    };

    // NOTE: Virtual functions don't mix well with templates
    // Overriden by child functions
    // virtual int run(const MapReduceTask<KM, VM, KR, VR>& task) = 0;

    bool isMaster() {
        return pid == MASTER_THREAD;
    }

protected:
    int pid; // Process ID (local to each thread)
    int nproc; // Number of processors
};

#endif /* MAP_REDUCE_H */