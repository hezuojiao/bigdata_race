//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_QUERYEXCUTOR_H
#define BIGDATA_RACE_QUERYEXCUTOR_H

#include <cstring>
#include <queue>

#include "spp.h"

#include "LeftTable.h"
#include "RightTable.h"

using spp::sparse_hash_map;

class Executor {
 private:
  int topn;
  int* c1Result;
  int* c2Result;
  int* c3Result;

  std::vector<int> o_orderkey;
  std::vector<int> o_orderdate;

  sparse_hash_map<int, sparse_hash_map<int, int>> result;

 public:
  explicit Executor(LeftTable* leftTable, RightTable* rightTable, char mktsegmentCondition,
      int orderdateCondition, int shipdateCondition, int topn) {

    leftTable->filterAfterHashJoin(mktsegmentCondition, orderdateCondition, o_orderkey, o_orderdate);
    rightTable->filterAfterSortMergeJoin(shipdateCondition, o_orderkey, o_orderdate, result);

    this->topn = topn;
    this->c1Result = new int[topn];
    this->c2Result = new int[topn];
    this->c3Result = new int[topn];
    memset(c1Result, 0, sizeof(int) * topn);
    memset(c2Result, 0, sizeof(int) * topn);
    memset(c3Result, 0, sizeof(int) * topn);
  }

  ~Executor() {
    delete[] c1Result;
    delete[] c2Result;
    delete[] c3Result;
  }

  char* getResult() {

    if (topn > MAX_LENGTH_INSERT_SORT) {
      heapSort();
    } else {
      insertSort();
    }

    auto char_buf = new char[40 * (topn + 1)];
    int pos = 0;
    pos += sprintf(char_buf, "l_orderkey|o_orderdate|revenue\n");
    for (int i = 0; i < topn; i++) {
      if (c3Result[i] > 0) {
        pos += sprintf(char_buf + pos,
            "%d|%d-%d-%d|%.2f\n", c1Result[i],
            c2Result[i]/10000, (c2Result[i] % 10000) / 100, c2Result[i] % 100,
            (double)c3Result[i]/100.0);
      } else {
        break;
      }
    }
    return char_buf;
  }

 private:
  /***
 * result :
 * ----------------------------------
 *|            | order_key          |
 * ----------------------------------
 *| order_date | extended_price_sum |
 * ----------------------------------
 */

  void insertSort() {
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


  void heapSort() {
    for (const auto& dateIter : result) {
      auto orderDate = dateIter.first;
      for (const auto& keyIter : dateIter.second) {
        auto orderKey = keyIter.first;
        auto expenseprice = keyIter.second;

        if (expenseprice < c3Result[0]) {
          continue;
        }

        c3Result[0] = expenseprice;
        c2Result[0] = orderDate;
        c1Result[0] = orderKey;
        adjust(topn);
      }
    }

    for (int i = topn - 1; i > 0; i--) {
      swap(0, i);
      adjust(i);
    }
  }


  inline void adjust(int end) {
    int i = 0, c;
    while (i < end) {
      c = i * 2 + 1;
      if (c >= end) break;
      if (c + 1 < end && c3Result[c] > c3Result[c + 1]) ++c;
      if (c3Result[i] > c3Result[c]) {
        swap(i, c);
        i = c;
      } else {
        break;
      }
    }
  }

  inline void swap(int left, int right) {
    c1Result[left] ^= c1Result[right] ^= c1Result[left] ^= c1Result[right];
    c2Result[left] ^= c2Result[right] ^= c2Result[left] ^= c2Result[right];
    c3Result[left] ^= c3Result[right] ^= c3Result[left] ^= c3Result[right];
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
