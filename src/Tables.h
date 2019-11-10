//
// Created by hezuojiao on 2019-10-22.
//

#ifndef BIGDATA_RACE_TABLES_H
#define BIGDATA_RACE_TABLES_H

#include <thread>

#include "phmap.h"
#include "phmap_dump.h"

#include "Constants.h"
#include "Utils.h"

using phmap::flat_hash_map;
using phmap::flat_hash_set;

class Table {
 public:
  virtual void parseColumns(const char* file_name) = 0;
  virtual void buildCache(const char* file_name, bool rebuild) = 0;
  virtual ~Table() = default;
};

class Customer : public Table {
 public:
  flat_hash_map<char, flat_hash_set<uint32_t>> c_hashtable;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
 private:
  void serialize();
  void deserialize();
};

class Order : public Table {
 public:
  uint32_t** o_orderkey;
  uint32_t** o_custkey;
  uint16_t** o_orderdate;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
};

class Lineitem : public Table {
 public:
  uint32_t** l_orderkey;
  uint32_t** l_extendedprice;
  uint16_t** l_shipdate;
  void parseColumns(const char* fileName) override;
  void buildCache(const char* fileName, bool rebuild) override;
};

#endif //BIGDATA_RACE_TABLES_H
