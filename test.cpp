#include "bitcask.hpp"
#include <thread>
#include <chrono>
#include <vector>
#include <pthread.h>

using namespace std;

void invoc1(Bitcask &test)
{
    for (int i = 0; i < 5; i++)
    {
        string key = std::to_string(i);                        //, value1 = "Cy is the fxxking God!";
        test.set(move(key), string("Cy is the fxxking God!")); // key + to_string(rand())
    }
}

void invoc2(Bitcask &test)
{
    for (int i = 5; i > 0; i--)
    {
        string key = std::to_string(i); //, value1 = "Cy the fxxking God!";
        test.set(move(key), string("Cy is the fxxking God.."));
    }
}

// extern Status status = Status(OK, string("success."));

int main()
{
    Bitcask test;
    auto t1 = std::chrono::high_resolution_clock::now();

    string value;
    // test.set("1", "cy is god");
    // test.set("2", "cy is god");
    // test.get("1", &value);
    // cout << value << endl;

    // test.remove("1");
    // cout << test.get("2") << endl;

    std::vector<std::thread> threads;
    threads.emplace_back(invoc1, std::ref(test));
    threads.emplace_back(invoc2, std::ref(test));
    threads.emplace_back(invoc1, std::ref(test));
    threads.emplace_back(invoc2, std::ref(test));

    for (auto &thread : threads)
        thread.join();

    auto t2 = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double, std::milli> fp_ms = t2 - t1;

    std::cout << "time cost: " << fp_ms.count() << " ms" << endl;

    test.print_kv();

    std::cout << "====================================" << endl
              << "recovery data from log:" << endl;

    Bitcask test_recovery;
    test_recovery.print_kv();

    return 0;
}
