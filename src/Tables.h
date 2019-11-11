//
// Created by hezuojiao on 2019-10-22.
//

#ifndef BIGDATA_RACE_TABLES_H
#define BIGDATA_RACE_TABLES_H

#include <thread>

#include "Constants.h"
#include "Utils.h"

class Order {
 public:
  uint32_t*** o_orderkey;
  uint16_t*** o_orderdate;
  void parseColumns(const char* c_filename, const char* o_filename);
  void buildCache(const char* c_filename, const char* o_filename, bool rebuild);
};

class Lineitem {
 public:
  uint32_t** l_orderkey;
  uint32_t** l_extendedprice;
  uint16_t** l_shipdate;
  void parseColumns(const char* l_filename);
  void buildCache(const char* l_filename, bool rebuild);
};

#endif //BIGDATA_RACE_TABLES_H
