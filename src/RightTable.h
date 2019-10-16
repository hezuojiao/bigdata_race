//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_RIGHTTABLE_H
#define BIGDATA_RACE_RIGHTTABLE_H

#include <unordered_set>
#include <unordered_map>
#include <string>
#include <vector>

#include "Constants.h"
#include "ReadBuffer.h"

class RightTable {
 private:
  int* l_orderkey;
  int* l_shipdate;
  double* l_extendedprice;
  int tablePosition;

 public:
  RightTable():tablePosition(0) {
    l_orderkey = new int[LINEITEM];
    l_shipdate = new int[LINEITEM];
    l_extendedprice = new double[LINEITEM];
  }


  ~RightTable() {
    delete[] l_orderkey;
    delete[] l_shipdate;
    delete[] l_extendedprice;
  }

  void buildTable(const char* lineitemFileName) {
    auto fd = open(lineitemFileName, O_RDONLY , 0777);
    auto buf = new char[BUFFER_SIZE];
    auto reader = new ReadBuffer(buf, fd);
    char ch;
    int num1, num2;
    double p;
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
        num2 = ch != '.' ? num2 * 10 + ch - '0' : num2;
      }
      p = (double) num2 / 100.0;
      num2 = 0;
      while (reader->hasNext()) {
        ch = reader->next();
        if (ch == '\n') break;
        num2 = ch != '-' ? num2 * 10 + ch - '0' : num2;
      }
      addRow(num1, p, num2);
    }

    delete reader;
    delete[] buf;
  }


  void getRight(int shipdateCondition, std::vector<int>& orderkey, std::vector<double>& extendedprice) {
    for (int i = 0; i < tablePosition; i++) {
      if (l_shipdate[i] > shipdateCondition) {
        orderkey.push_back(l_orderkey[i]);
        extendedprice.push_back(l_extendedprice[i]);
      }
    }
  }

 private:
  void addRow(int orderKey, double extendedPrice, int shipDate) {
    l_orderkey[tablePosition] = orderKey;
    l_extendedprice[tablePosition] = extendedPrice;
    l_shipdate[tablePosition] = shipDate;
    ++tablePosition;
  }

};


#endif //BIGDATA_RACE_RIGHTTABLE_H
