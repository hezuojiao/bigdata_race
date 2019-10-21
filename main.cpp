#include <iostream>
#include <thread>

#include "src/QueryExecutor.h"

int main(int argc, char** argv) {

  auto leftTable = new LeftTable();
  auto rightTable = new RightTable();

  typedef std::chrono::duration<float> float_seconds;
  auto start = std::chrono::system_clock::now();
  std::thread thread1 = std::thread([leftTable, argv] {
    leftTable->buildTable(argv[1], argv[2]);
  });
  std::thread thread2 = std::thread([rightTable, argv] {
    rightTable->buildTable(argv[3]);
  });
  thread1.join();
  thread2.join();
  auto end = std::chrono::system_clock::now();
  auto dur = end - start;
  auto secs = std::chrono::duration_cast<float_seconds>(dur);
  printf("LOG :: build spend: %fs\n", secs.count());

  auto round = std::stoi(argv[4]);
  int max_thread_num = std::thread::hardware_concurrency();
  auto thread_num = std::min(max_thread_num, round);
  std::thread threads[thread_num];
  std::vector<char*> results(round);

  start = std::chrono::system_clock::now();
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([leftTable, rightTable, i, round, thread_num, &results, &argv]{
      for (int round_id = i; round_id < round; round_id += thread_num) {
        auto mktsegmentCondition = argv[5 + round_id * 4][0];
        auto orderdateCondition = Executor::DateToInt(argv[5 + round_id * 4 + 1]);
        auto shipdateCondition = Executor::DateToInt(argv[5 + round_id * 4 + 2]);
        auto topn = std::stoi(argv[5 + round_id * 4 + 3]);
        auto executor = new Executor(leftTable, rightTable, mktsegmentCondition, orderdateCondition, shipdateCondition, topn);
        results[round_id] = executor->getResult();
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  for (auto &r : results) {
    printf("%s", r);
  }

  end = std::chrono::system_clock::now();
  dur = end - start;
  secs = std::chrono::duration_cast<float_seconds>(dur);

  printf("LOG :: query spend: %fs\n", secs.count());

  return 0;
}