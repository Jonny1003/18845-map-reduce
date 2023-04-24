/**
 * @file util.h
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Utilities and miscellaneous stuff
 * @version 0.1
 * @date 2023-04-22
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#ifndef UTIL_H
#define UTIL_H

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

#endif /* UTIL_H */