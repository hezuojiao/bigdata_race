#include <iostream>
#include <thread>

#include "src/Database.h"

int main(int argc, char* argv[]) {

  mkdir(DATA_PATH.c_str(), 0777);

  Database tpch;

  // load
  tpch.importDB(argv[1], argv[2], argv[3]);

  auto round = std::stoi(argv[4]);
  auto thread_num = std::min(MAX_CORE_NUM, round);
  std::thread threads[thread_num];
  char* results[round];

  // query
  for (int i = 0; i < thread_num; i++) {
    threads[i] = std::thread([&, i]{
      for (int round_id = i; round_id < round; round_id += thread_num) {
        auto mktsegmentCondition = argv[5 + round_id * 4][0];
        auto orderdateCondition = util::DateToInt(argv[5 + round_id * 4 + 1]);
        auto shipdateCondition = util::DateToInt(argv[5 + round_id * 4 + 2]);
        auto topn = std::stoi(argv[5 + round_id * 4 + 3]);
        results[round_id] = tpch.query(mktsegmentCondition, orderdateCondition, shipdateCondition, topn);
      }
    });
  }

  for (auto &t : threads) {
    t.join();
  }

  for (auto &r : results) {
    printf("%s", r);
  }

  return 0;
}