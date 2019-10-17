//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_LEFTTABLE_H
#define BIGDATA_RACE_LEFTTABLE_H


#include <unordered_set>
#include <unordered_map>
#include <string>
#include <vector>

#include "Constants.h"
#include "ReadBuffer.h"

class LeftTable {
 private:
  int* o_orderkey;
  int* o_custkey;
  int* o_orderdate;
  int tablePosition;
  std::unordered_map<char, std::unordered_set<int>> c_hashtable;

 public:
  LeftTable():tablePosition(0) {
    o_orderkey = new int[ORDER];
    o_custkey = new int[ORDER];
    o_orderdate = new int[ORDER];
  }


  ~LeftTable() {
    delete[] o_orderkey;
    delete[] o_custkey;
    delete[] o_orderdate;
  }

  void buildTable(const char* customerFileName, const char* orderFileName) {
    auto fd = open(customerFileName, O_RDONLY , 0777);
    auto buf = new char[BUFFER_SIZE];
    auto reader = new ReadBuffer(buf, fd);
    char ch, name;
    int num1 = 1, num2, num3;
    while (reader->hasNext()) {
      while (reader->next() != '|') {}
      name = reader->next();
      while (reader->hasNext() && reader->next() != '\n'){}
      c_hashtable[name].insert(num1++);
    }

    delete reader;

    fd = open(orderFileName, O_RDONLY, 0777);
    reader = new ReadBuffer(buf, fd);

    while (reader->hasNext()) {
      num1 = 0;
      while (reader->hasNext()) {
        ch = reader->next();
        if (ch == '|') break;
        num1 = num1 * 10 + ch - '0';
      }
      num2 = 0;
      while (reader->hasNext()) {
        ch = reader->next();
        if (ch == '|') break;
        num2 = num2 * 10 + ch - '0';
      }
      num3 = 0;
      while (reader->hasNext()) {
        ch = reader->next();
        if (ch == '\n') break;
        num3 = ch != '-' ? num3 * 10 + ch - '0' : num3;
      }
      addRow(num1, num2, num3);
    }

    delete reader;
    delete[] buf;
  }

  void filterAfterHashJoin(char mktsegmentCondition, int orderdateCondition,
      std::vector<int>& orderkey, std::vector<int>& orderdate) {
    auto c_custkey = c_hashtable[mktsegmentCondition];
    for (int i = 0; i < tablePosition; i++) {
      if (c_custkey.find(o_custkey[i]) != c_custkey.end() && o_orderdate[i] < orderdateCondition) {
        orderkey.push_back(o_orderkey[i]);
        orderdate.push_back(o_orderdate[i]);
      }
    }
  }

 private:
  void addRow(int orderKey, int custKey, int orderDate) {
    o_orderkey[tablePosition] = orderKey;
    o_custkey[tablePosition] = custKey;
    o_orderdate[tablePosition] = orderDate;
    tablePosition++;
  }
};

#endif //BIGDATA_RACE_LEFTTABLE_H
