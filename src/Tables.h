//
// Created by hezuojiao on 2019-10-22.
//

#ifndef BIGDATA_RACE_TABLES_H
#define BIGDATA_RACE_TABLES_H

#include "phmap.h"
#include "phmap_dump.h"

#include "Constants.h"
#include "Utils.h"

using phmap::flat_hash_map;
using phmap::flat_hash_set;

class Table {
 public:
  size_t position = 0;
  virtual void parseColumns(const char* file_name) = 0;
  virtual void buildCache(const char* file_name, bool rebuild) = 0;
  virtual ~Table() = default;
};

class Customer : public Table {
 public:
  flat_hash_map<char, flat_hash_set<uint32_t>> c_hashtable;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
//  inline sparse_hash_set<int>&operator[](const char key) {
//    return c_hashtable[key];
//  };
 private:
  void serialize();
  void deserialize();
};

class Order : public Table {
 public:
  uint32_t* o_orderkey;
  uint32_t* o_custkey;
  uint32_t* o_orderdate;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
 private:
  inline void addRow(uint32_t orderKey, uint32_t custKey, uint32_t orderDate) {
    o_orderkey[position] = orderKey;
    o_custkey[position] = custKey;
    o_orderdate[position] = orderDate;
    ++position;
  }
};

class Lineitem : public Table {
 public:
  uint32_t* l_orderkey;
  uint32_t* l_shipdate;
  uint32_t* l_extendedprice;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
 public:
  inline void addRow(uint32_t orderKey, uint32_t extendedPrice, uint32_t shipDate) {
    l_orderkey[position] = orderKey;
    l_extendedprice[position] = extendedPrice;
    l_shipdate[position] = shipDate;
    ++position;
  }
};

#endif //BIGDATA_RACE_TABLES_H
