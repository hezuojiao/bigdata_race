#include <iostream>
#include <thread>

#include "src/Executor.h"

int main(int argc, char* argv[]) {

  auto customer = new Customer();
  auto order = new Order();
  auto lineitem = new Lineitem();

  bool rebuild = !util::file_exists(O_ORDERKEY_PATH.c_str());
  typedef std::chrono::duration<float> float_seconds;
  auto start = std::chrono::system_clock::now();
  std::thread thread1 = std::thread([customer, rebuild, argv] {
    customer->buildCache(argv[1], rebuild);
  });
  std::thread thread2 = std::thread([order, rebuild, argv] {
    order->buildCache(argv[2], rebuild);
  });
  std::thread thread3 = std::thread([lineitem, rebuild, argv] {
    lineitem->buildCache(argv[3], rebuild);
  });
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
    threads[i] = std::thread([customer, order, lineitem, i, round, thread_num, &results, &argv]{
      for (int round_id = i; round_id < round; round_id += thread_num) {
        auto mktsegmentCondition = argv[5 + round_id * 4][0];
        auto orderdateCondition = util::DateToInt(argv[5 + round_id * 4 + 1]);
        auto shipdateCondition = util::DateToInt(argv[5 + round_id * 4 + 2]);
        auto topn = std::stoi(argv[5 + round_id * 4 + 3]);
        auto executor = new Executor(customer, order, lineitem, mktsegmentCondition, orderdateCondition, shipdateCondition, topn);
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