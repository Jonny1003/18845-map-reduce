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
#include "text_gen_task.h"
#include "word_count_task.h"
#include "grep.h"
#include "mini_basic_map_reduce.h"
#include "mini_tree_map_reduce.h"
#include <map>
#include <chrono>

int main(int argc, char *argv[])
{
    // std::map<std::string, int> kvs({{"a", 42}, {"b", 101}, {"c", 17}, {"d", 13}});
    // std::string outLoc = "./out";
    // IdentityTask idTask(std::move(kvs), outLoc);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // mapReduce.run<IdentityTask>(idTask);

    // std::map<std::string, int> kvs({{"a", 42}, {"b", 101}, {"c", 17}, {"d", 13}});
    // std::string outLoc = "./out";
    // DoublingTask idTask(std::move(kvs), outLoc);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // mapReduce.run<DoublingTask>(idTask);

    // std::map<std::string, std::string> kvs;
    // for (int i = 0; i < 1000; ++i) {
    //     kvs.emplace(std::to_string(i), "");
    // }
    // std::string outLoc = "./out";
    // TextGenTask task(std::move(kvs), outLoc);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // mapReduce.run<TextGenTask>(task);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // std::map<std::string, std::string> kvs;
    // if (mapReduce.isMaster()) {
    //     for (int i = 0; i < 80; ++i) {
    //         kvs.emplace(std::to_string(i), "./out/" + std::to_string(i));
    //     }
    // }
    // std::string outLoc = "./results";
    // WordCountTask task(std::move(kvs), outLoc);

    // mapReduce.run<WordCountTask>(task);

    // Grep::TaskType kvs;
    // for (int i = 0; i < 80; ++i) {
    //     kvs.emplace("./out/" + std::to_string(i), "fried");
    // }
    // std::string outLoc = "./results";
    // Grep task(std::move(kvs), outLoc);

    // MiniBasicMapReduce mapReduce (argc, argv);

    // mapReduce.run<Grep>(task);


    if (argv[0][0] == 't') {
        std::cout << "RUNNING TREE VERSION\n";
        std::map<std::string, std::string> kvs;
        for (int i = 0; i < 1000; ++i) {
            kvs.emplace(std::to_string(i), "");
        }
        std::string outLoc = "./out";
        TextGenTask task(std::move(kvs), outLoc);

        MiniTreeMapReduce mapReduce (argc, argv);

        auto start = std::chrono::high_resolution_clock::now();
        mapReduce.run<TextGenTask>(task);
        if (mapReduce.isMaster()) {
            const auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Text generation time: " <<
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us\n";
        }

        std::map<std::string, std::string> kvs2;
        if (mapReduce.isMaster()) {
            for (int i = 0; i < 1000; ++i) {
                kvs2.emplace(std::to_string(i), "./out/" + std::to_string(i));
            }
        }
        std::string outLoc2 = "./results";
        WordCountTask t(std::move(kvs2), outLoc2);

        start = std::chrono::high_resolution_clock::now();
        mapReduce.run<WordCountTask>(t);
        if (mapReduce.isMaster()) {
            const auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Word count time: " <<
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us\n";
        }
    } else {
        std::map<std::string, std::string> kvs;
        for (int i = 0; i < 1000; ++i) {
            kvs.emplace(std::to_string(i), "");
        }
        std::string outLoc = "./out";
        TextGenTask task(std::move(kvs), outLoc);

        MiniBasicMapReduce mapReduce (argc, argv);

        auto start = std::chrono::high_resolution_clock::now();
        mapReduce.run<TextGenTask>(task);
        if (mapReduce.isMaster()) {
            const auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Text generation time: " <<
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us\n";
        }

        std::map<std::string, std::string> kvs2;
        if (mapReduce.isMaster()) {
            for (int i = 0; i < 1000; ++i) {
                kvs2.emplace(std::to_string(i), "./out/" + std::to_string(i));
            }
        }
        std::string outLoc2 = "./results";
        WordCountTask t(std::move(kvs2), outLoc2);

        start = std::chrono::high_resolution_clock::now();
        mapReduce.run<WordCountTask>(t);
        if (mapReduce.isMaster()) {
            const auto end = std::chrono::high_resolution_clock::now();
            std::cout << "Word count time: " <<
                std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "us\n";
        }
    }

    return 0;
}