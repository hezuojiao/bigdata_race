//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_EXECUTOR_H
#define BIGDATA_RACE_EXECUTOR_H

#include <cstring>
#include <vector>

#include "phmap.h"
#include "Tables.h"

using phmap::flat_hash_map;

class Executor {
 private:
  int topn;
  int* c1Result;
  int* c2Result;
  int* c3Result;

  flat_hash_map<uint64_t, int> result;

 public:
   Executor(Customer* customer, const Order* order, const Lineitem* lineitem,
      char mktsegmentCondition, int orderdateCondition, int shipdateCondition, int topn) {

    this->executePlan(customer, order, lineitem, mktsegmentCondition, orderdateCondition, shipdateCondition);

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
                       "%d|%d-%02d-%02d|%.2f\n", c1Result[i],
                       c2Result[i] / 10000, (c2Result[i] % 10000) / 100, c2Result[i] % 100,
                       (double) c3Result[i] / 100.0);
      } else {
        break;
      }
    }
    return char_buf;
  }

 private:

  void executePlan(Customer* customer, const Order* order, const Lineitem* lineitem,
                    char mktsegmentCondition, int orderdateCondition, int shipdateCondition) {
    auto &c_custkey = customer->c_hashtable[mktsegmentCondition];
    uint32_t o_pos = 0, l_pos = 0;
    while (o_pos < ORDER && l_pos < LINEITEM) {
      if (order->o_orderdate[o_pos] < orderdateCondition) {
        auto o_key = order->o_orderkey[o_pos];
        while (lineitem->l_orderkey[l_pos] < o_key) {++l_pos;}
        while (l_pos < LINEITEM && lineitem->l_orderkey[l_pos] == o_key) {
          if (lineitem->l_shipdate[l_pos] > shipdateCondition && c_custkey.find(order->o_custkey[o_pos]) != c_custkey.end()) {
            uint64_t key = (((uint64_t)o_key)<<32) | (order->o_orderdate[o_pos]);
            result[key] += lineitem->l_extendedprice[l_pos];
          }
          ++l_pos;
        }
      }
      ++o_pos;
    }
  }

  void executePlan2(Customer* customer, const Order* order, const Lineitem* lineitem,
                   char mktsegmentCondition, int orderdateCondition, int shipdateCondition) {

    auto &c_custkey = customer->c_hashtable[mktsegmentCondition];
    std::vector<int> orderkey; orderkey.reserve(15000000);
    std::vector<int> orderdate; orderdate.reserve(15000000);

    for (int i = 0; i < ORDER; i++) {  // filter and hash join
      if (order->o_orderdate[i] < orderdateCondition && c_custkey.find(order->o_custkey[i]) != c_custkey.end()) {
        orderkey.push_back(order->o_orderkey[i]);
        orderdate.push_back(order->o_orderdate[i]);
      }
    }

    uint32_t o_pos = 0, l_pos = 0, n1 = orderkey.size();
    while (o_pos < n1 && l_pos < LINEITEM) { // sort merge join and filter
      auto o_key = orderkey[o_pos];
      auto l_key = lineitem->l_orderkey[l_pos];
      if (o_key < l_key) {
        ++o_pos;
      } else if (o_key > l_key) {
        ++l_pos;
      } else {
        if (lineitem->l_shipdate[l_pos] > shipdateCondition) {
          uint64_t key = (((uint64_t)o_key)<<32) | (order->o_orderdate[o_pos]);
          result[key] += lineitem->l_extendedprice[l_pos];
        }
        ++l_pos;
      }
    }
  }

  /***
 * result :
 * ----------------------------------
 *|            | order_key          |
 * ----------------------------------
 *| order_date | extended_price_sum |
 * ----------------------------------
 */

  void insertSort() {

    for (const auto& iter : result) {

      auto expenseprice = iter.second;

      if (expenseprice < c3Result[topn - 1]) {
        continue;
      }

      auto key = iter.first;
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
      c2Result[i] = key & (0xffffffff);
      c1Result[i] = (key >> 32);
    }

  }


  void heapSort() {

    for (const auto& iter : result) {

      auto expenseprice = iter.second;

      if (expenseprice < c3Result[0]) {
        continue;
      }

      auto key = iter.first;
      c3Result[0] = expenseprice;
      c2Result[0] = key & (0xffffffff);
      c1Result[0] = (key >> 32);
      adjust(topn);
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
};

#endif //BIGDATA_RACE_EXECUTOR_H
