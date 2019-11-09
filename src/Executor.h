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
  Customer* customer;
  const Order* order;
  const Lineitem* lineitem;
  const uint32_t orderdateCondition;
  const uint32_t shipdateCondition;
  const uint16_t topn;
  const char mktsegmentCondition;

  uint32_t* c1Result;
  uint32_t* c2Result;
  uint32_t* c3Result;

  std::vector<flat_hash_map<uint64_t, uint32_t>> results;

 public:
   Executor(Customer* customer, Order* order, Lineitem* lineitem,
      char mktsegmentCondition, uint32_t orderdateCondition, uint32_t shipdateCondition, uint16_t topn) :
      customer(customer), order(order), lineitem(lineitem),
      orderdateCondition(orderdateCondition), shipdateCondition(shipdateCondition), topn(topn), mktsegmentCondition(mktsegmentCondition) {

     std::thread threads[MAX_CORE_NUM];
     results.reserve(MAX_CORE_NUM);
     for (int i = 0; i < MAX_CORE_NUM; ++i) {
       results.emplace_back(flat_hash_map<uint64_t, uint32_t>());
       threads[i] = std::thread([i, this] { executePlan(i);});
     }

     this->c1Result = new uint32_t[topn];
     this->c2Result = new uint32_t[topn];
     this->c3Result = new uint32_t[topn];
     memset(c3Result, 0, sizeof(uint32_t) * topn);

     for (auto &t : threads) {
       t.join();
     }
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
                       "%u|%d-%02d-%02d|%.2f\n", c1Result[i],
                       c2Result[i] / 10000, (c2Result[i] % 10000) / 100, c2Result[i] % 100,
                       (double) c3Result[i] / 100.0);
      } else {
        break;
      }
    }
    return char_buf;
  }

 private:

  void executePlan(int thread_id) {
    auto &c_custkey = customer->c_hashtable[mktsegmentCondition];
    uint32_t o_pos = 0, l_pos = 0;
    while (o_pos < ORDER && l_pos < LINEITEMS[thread_id]) {
      if (order->o_orderdate[thread_id][o_pos] < orderdateCondition) {
        auto o_key = order->o_orderkey[thread_id][o_pos];
        while (lineitem->l_orderkey[thread_id][l_pos] < o_key) {++l_pos;}
        while (l_pos < LINEITEMS[thread_id] && lineitem->l_orderkey[thread_id][l_pos] == o_key) {
          if (lineitem->l_shipdate[thread_id][l_pos] > shipdateCondition
          && c_custkey.find(order->o_custkey[thread_id][o_pos]) != c_custkey.end()) {
            uint64_t key = (((uint64_t)o_key)<<32) | (order->o_orderdate[thread_id][o_pos]);
            results[thread_id][key] += lineitem->l_extendedprice[thread_id][l_pos];
          }
          ++l_pos;
        }
      }
      ++o_pos;
    }
  }

//  void executePlan(Customer* customer, const Order* order, const Lineitem* lineitem,
//                   char mktsegmentCondition, int orderdateCondition, int shipdateCondition) {
//
//    auto &c_custkey = customer->c_hashtable[mktsegmentCondition];
//    std::vector<uint32_t> orderkey; orderkey.reserve(15000000);
//    std::vector<uint32_t> orderdate; orderdate.reserve(15000000);
//
//    for (int i = 0; i < ORDER; i++) {  // filter and hash join
//      if (order->o_orderdate[i] < orderdateCondition && c_custkey.find(order->o_custkey[i]) != c_custkey.end()) {
//        orderkey.push_back(order->o_orderkey[i]);
//        orderdate.push_back(order->o_orderdate[i]);
//      }
//    }
//
//    uint32_t o_pos = 0, l_pos = 0, n1 = orderkey.size();
//    while (o_pos < n1 && l_pos < LINEITEM) { // sort merge join and filter
//      auto o_key = orderkey[o_pos];
//      auto l_key = lineitem->l_orderkey[l_pos];
//      if (o_key < l_key) {
//        ++o_pos;
//      } else if (o_key > l_key) {
//        ++l_pos;
//      } else {
//        if (lineitem->l_shipdate[l_pos] > shipdateCondition) {
//          uint64_t key = (((uint64_t)o_key)<<32) | (order->o_orderdate[o_pos]);
//          result[key] += lineitem->l_extendedprice[l_pos];
//        }
//        ++l_pos;
//      }
//    }
//  }

  /***
 * result :
 * ----------------------------------
 *|            | order_key          |
 * ----------------------------------
 *| order_date | extended_price_sum |
 * ----------------------------------
 */

  void insertSort() {
    for (const auto& result : results) {
      for (const auto &iter : result) {

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
  }


  void heapSort() {
    for (const auto& result : results) {
      for (const auto &iter : result) {

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
