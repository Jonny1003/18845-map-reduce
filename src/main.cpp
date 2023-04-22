/**
 * @file main.cpp
 * @author Jonathan Ke (jak2@andrew.cmu.edu)
 * @brief Wrapper for selecting map reduce tasks to run
 * @version 0.1
 * @date 2023-04-20
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#include <cstring>
#include <algorithm>
#include <iostream>
#include "identity_task.h"
#include "doubling_task.h"
#include "mini_basic_map_reduce.h"
#include <map>

int main(int argc, char *argv[])
{
    // std::map<std::string, int> kvs({{"a", 42}, {"b", 101}, {"c", 17}, {"d", 13}});
    // std::string outLoc = "./out";
    // IdentityTask idTask(std::move(kvs), outLoc);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // mapReduce.run<IdentityTask>(idTask);

    std::map<std::string, int> kvs({{"a", 42}, {"b", 101}, {"c", 17}, {"d", 13}});
    std::string outLoc = "./out";
    DoublingTask idTask(std::move(kvs), outLoc);

    MiniBasicMapReduce mapReduce (argc, argv);

    mapReduce.run<DoublingTask>(idTask);
    return 0;
}