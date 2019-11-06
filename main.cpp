#include <iostream>
#include <thread>

#include "src/Executor.h"

int main(int argc, char* argv[]) {

  if (mkdir(DATA_PATH.c_str(), 0777) == -1) {
    printf("LOG :: mkdir %s failed! : Permission denied\n", DATA_PATH.c_str());
    exit(-1);
  }

  Customer customer;
  Order order;
  Lineitem lineitem;

  bool rebuild = !util::file_exists(O_ORDERKEY_PATH.c_str());
  typedef std::chrono::duration<float> float_seconds;
  auto start = std::chrono::system_clock::now();
  std::thread thread1 = std::thread([&] { customer.buildCache(argv[1], rebuild); });
  std::thread thread2 = std::thread([&] { order.buildCache(argv[2], rebuild); });
  std::thread thread3 = std::thread([&] { lineitem.buildCache(argv[3], rebuild); });
  thread1.join();
  thread2.join();
  thread3.join();
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
    threads[i] = std::thread([&, i] {
      for (int round_id = i; round_id < round; round_id += thread_num) {
        auto mktsegmentCondition = argv[5 + round_id * 4][0];
        auto orderdateCondition = util::DateToInt(argv[5 + round_id * 4 + 1]);
        auto shipdateCondition = util::DateToInt(argv[5 + round_id * 4 + 2]);
        auto topn = std::stoi(argv[5 + round_id * 4 + 3]);
        Executor executor(customer, order, lineitem, mktsegmentCondition, orderdateCondition, shipdateCondition, topn);
        results[round_id] = executor.getResult();
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