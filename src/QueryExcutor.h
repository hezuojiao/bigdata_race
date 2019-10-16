//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_QUERYEXCUTOR_H
#define BIGDATA_RACE_QUERYEXCUTOR_H

#include "LeftTable.h"
#include "RightTable.h"
#include <cstring>

class Executor {
 private:
  int topn;
  int* c1Result;
  int* c2Result;
  double* c3Result;

  std::vector<int> o_orderkey;
  std::vector<int> o_orderdate;
  std::vector<int> l_orderkey;
  std::vector<double > l_extendedprice;

  std::unordered_map<int, std::unordered_map<int, double>> result;

 public:
  explicit Executor(LeftTable* leftTable, RightTable* rightTable, char mktsegmentCondition,
      int orderdateCondition, int shipdateCondition, int topn) {

    leftTable->getLeft(mktsegmentCondition, orderdateCondition, o_orderkey, o_orderdate);
    rightTable->getRight(shipdateCondition, l_orderkey, l_extendedprice);
    this->topn = topn;
    this->c1Result = new int[topn];
    this->c2Result = new int[topn];
    this->c3Result = new double[topn];
    memset(c1Result, 0, sizeof(int) * topn);
    memset(c2Result, 0, sizeof(int) * topn);
    memset(c3Result, 0, sizeof(double) * topn);
  }

  ~Executor() {
    delete[] c1Result;
    delete[] c2Result;
    delete[] c3Result;
  }

  std::string execute() {
    sortMergeJoin();
    sortResult();
    return getResult();
  }

 private:
  void sortMergeJoin() {
    int pos1 = 0, pos2 = 0;
    int n1 = o_orderkey.size(), n2 = l_orderkey.size();
    printf("candidate: %d, %d\n", n1, n2);
    while (pos1 < n1 && pos2 < n2) {
      int o_key = o_orderkey[pos1];
      int l_key = l_orderkey[pos2];
      if (o_key < l_key) {
        ++pos1;
      } else if (o_key > l_key) {
        ++pos2;
      } else {
        addRow(o_key, o_orderdate[pos1], l_extendedprice[pos2]);
        ++pos2;
      }
    }
  }

  void addRow(int orderkey, int orderdate, double extendedprice) {
    result[orderdate][orderkey] += extendedprice;
  }

  /***
 * result :
 * ----------------------------------
 *|            | order_key          |
 * ----------------------------------
 *| order_date | extended_price_sum |
 * ----------------------------------
 */
  void sortResult() {
    for (const auto& dateIter : result) {
      auto orderDate = dateIter.first;
      for (const auto& keyIter : dateIter.second) {
        auto orderKey = keyIter.first;
        auto expenseprice = keyIter.second;

        if (expenseprice < c3Result[topn - 1]) {
          continue;
        }

        int i = topn - 1;
        for (; i > 0; i--) {
          if (c3Result[i - 1] < expenseprice) {
            c3Result[i] = c3Result[i - 1];
            c2Result[i] = c2Result[i - 1];
            c1Result[i] = c1Result[i - 1];
          } else {
            break;
          }
        }
        c3Result[i] = expenseprice;
        c2Result[i] = orderDate;
        c1Result[i] = orderKey;
      }
    }
  }

  std::string getResult() {
    std::string sb("l_orderkey|o_orderdate|revenue\n");
    for (int i = 0; i < topn; i++) {
      if (c3Result[i] > 0) {
        sb += std::to_string(c1Result[i]) + "|" +
              std::to_string(c2Result[i] / 10000) + "-" +
              std::to_string((c2Result[i] % 10000) / 100) + "-" +
              std::to_string(c2Result[i] % 100) + "|" +
              std::to_string(c3Result[i]) + "\n"; // TODO sprintf
      }
    }
    return sb;
  }

 public:
  static int DateToInt(const char* date) {
    int i_date = 0;
    for (int i = 0; date[i] != '\0'; i++) {
      i_date = date[i] != '-' ? i_date * 10 + date[i] - '0' : i_date;
    }
    return i_date;
  }
};

#endif //BIGDATA_RACE_QUERYEXCUTOR_H
