#include <iostream>
#include <thread>

#include "src/QueryExcutor.h"

int main(int argc, char** argv) {

  auto leftTable = new LeftTable();
  auto rightTable = new RightTable();

  typedef std::chrono::duration<float> float_seconds;
  auto start = std::chrono::system_clock::now();
  leftTable->buildTable(argv[1], argv[2]);
  rightTable->buildTable(argv[3]);
  auto end = std::chrono::system_clock::now();
  auto dur = end - start;
  auto secs = std::chrono::duration_cast<float_seconds>(dur);
  std::cout << "build spend: " << secs.count() << std::endl;

  start = std::chrono::system_clock::now();
  auto round = std::stoi(argv[4]);
  for (int i = 0; i < round; i++) {

    auto mktsegmentCondition = argv[5 + i * 4][0];
    auto orderdateCondition = Executor::DateToInt(argv[5 + i * 4 + 1]);
    auto shipdateCondition = Executor::DateToInt(argv[5 + i * 4 + 2]);
    auto topn = std::stoi(argv[5 + i * 4 + 3]);
    auto executor = new Executor(leftTable, rightTable, mktsegmentCondition, orderdateCondition, shipdateCondition, topn);
    std::cout << executor->execute();
  }
  end = std::chrono::system_clock::now();
  dur = end - start;
  secs = std::chrono::duration_cast<float_seconds>(dur);
  std::cout << "query spend: " << secs.count() << std::endl;

  return 0;
}