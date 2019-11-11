#include <iostream>
#include <thread>

#include "src/Executor.h"

int main(int argc, char* argv[]) {

  mkdir(DATA_PATH.c_str(), 0777);

  auto order = new Order();
  auto lineitem = new Lineitem();

  bool rebuild = !util::file_exists((O_ORDERKEY_PATH + "0").c_str());
  typedef std::chrono::duration<float> float_seconds;
  auto start = std::chrono::system_clock::now();
  std::thread thread1 = std::thread([&] { order->buildCache(argv[1], argv[2], rebuild); });
  std::thread thread2 = std::thread([&] { lineitem->buildCache(argv[3], rebuild); });
  thread1.join();
  thread2.join();
  auto end = std::chrono::system_clock::now();
  auto dur = end - start;
  auto secs = std::chrono::duration_cast<float_seconds>(dur);
  printf("LOG :: build spend: %fs\n", secs.count());

  start = std::chrono::system_clock::now();
  auto round = std::stoi(argv[4]);
  for (int i = 0; i < round; ++i) {
    auto p = util::get_key_index(argv[5 + i * 4][0]);
    auto orderdateCondition = util::DateToInt(argv[5 + i * 4 + 1]);
    auto shipdateCondition = util::DateToInt(argv[5 + i * 4 + 2]);
    auto topn = std::stoi(argv[5 + i * 4 + 3]);
    Executor executor(lineitem, order->o_orderkey[p], order->o_orderdate[p], orderdateCondition, shipdateCondition, topn);
    printf("%s", executor.getResult());
  }

  end = std::chrono::system_clock::now();
  dur = end - start;
  secs = std::chrono::duration_cast<float_seconds>(dur);

  printf("LOG :: query spend: %fs\n", secs.count());

  return 0;
}