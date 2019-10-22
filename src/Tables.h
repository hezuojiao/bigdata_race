//
// Created by hezuojiao on 2019-10-22.
//

#ifndef BIGDATA_RACE_TABLES_H
#define BIGDATA_RACE_TABLES_H

#include <spp.h>
#include "Constants.h"
#include "Utils.h"

using spp::sparse_hash_map;
using spp::sparse_hash_set;

class Table {
 public:
  size_t position = 0;
  virtual void parseColumns(const char* file_name) = 0;
  virtual void buildCache(const char* file_name, bool rebuild) = 0;
  virtual ~Table() = default;
};

class Customer : public Table {
 public:
  char* c_mktsegment;
  sparse_hash_map<char, sparse_hash_set<int>> c_hashtable;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
//  inline sparse_hash_set<int>&operator[](const char key) {
//    return c_hashtable[key];
//  };
};

class Order : public Table {
 public:
  int* o_orderkey;
  int* o_custkey;
  int* o_orderdate;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
 private:
  inline void addRow(int orderKey, int custKey, int orderDate) {
    o_orderkey[position] = orderKey;
    o_custkey[position] = custKey;
    o_orderdate[position] = orderDate;
    ++position;
  }
};

class Lineitem : public Table {
 public:
  int* l_orderkey;
  int* l_shipdate;
  int* l_extendedprice;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
 public:
  inline void addRow(int orderKey, int extendedPrice, int shipDate) {
    l_orderkey[position] = orderKey;
    l_extendedprice[position] = extendedPrice;
    l_shipdate[position] = shipDate;
    ++position;
  }
};

#endif //BIGDATA_RACE_TABLES_H
