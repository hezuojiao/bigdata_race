//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_RIGHTTABLE_H
#define BIGDATA_RACE_RIGHTTABLE_H


#include <string>
#include <vector>
#include <fstream>
#include <stdlib.h>
#include "errno.h"
#include "sys/stat.h"
#include "spp.h"

#include "Constants.h"
#include "ReadBuffer.h"

using spp::sparse_hash_map;

class RightTable {
 private:
  int* l_orderkey;
  int* l_shipdate;
  int* l_extendedprice;
  int tablePosition;

 public:
  RightTable():tablePosition(0) {
    l_orderkey = new int[LINEITEM];
    l_shipdate = new int[LINEITEM];
    l_extendedprice = new int[LINEITEM];
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
    int num1, num2, num3;
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

  void buildTable2(const char* lineitemFileName) {
    auto fd = open(lineitemFileName, O_RDONLY, 0777);
    struct stat s;
    if (fstat(fd, &s) < 0) {
      close(fd);
      return;
    }

    size_t len = s.st_size, pos = 0;
    char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
    posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
    int num1, num2, num3;
    while (pos < len) {
      num1 = 0;
      while (base[pos] != '|') {
        num1 = num1 * 10 + base[pos++] - '0';
      }
      ++pos;
      num2 = 0;
      while (base[pos] != '|') {
        num2 = num2 * 10 + base[pos++] - '0';
      }
      ++pos;
      num3 = 0;
      while (pos < len && base[pos] != '\n') {
        if (base[pos] != '-')
          num3 = num3 * 10 + base[pos] - '0';
        ++pos;
      }
      addRow(num1, num2, num3);
    }
    munmap(base, s.st_size);
    close(fd);
  }


  void filter(int shipdateCondition, std::vector<int>& orderkey, std::vector<int>& extendedprice) {
    for (int i = 0; i < tablePosition; i++) {
      if (l_shipdate[i] > shipdateCondition) {
        orderkey.push_back(l_orderkey[i]);
        extendedprice.push_back(l_extendedprice[i]);
      }
    }
  }

  void filterAfterSortMergeJoin(int shipdateCondition, const std::vector<int>& o_orderkey, const std::vector<int>& o_orderdate,
      sparse_hash_map<int, sparse_hash_map<int, int>>& result) {
      int pos1 = 0, pos2 = 0;
      int n1 = o_orderkey.size(), n2 = tablePosition;
      while (pos1 < n1 && pos2 < n2) {
        int o_key = o_orderkey[pos1];
        int l_key = l_orderkey[pos2];
        if (o_key < l_key) {
          ++pos1;
        } else if (o_key > l_key) {
          ++pos2;
        } else {
          if (l_shipdate[pos2] > shipdateCondition) {
            result[o_orderdate[pos1]][o_key] += l_extendedprice[pos2];
          }
          ++pos2;
        }
      }
  }

 private:
  void addRow(int orderKey, int extendedPrice, int shipDate) {
    l_orderkey[tablePosition] = orderKey;
    l_extendedprice[tablePosition] = extendedPrice;
    l_shipdate[tablePosition] = shipDate;
    ++tablePosition;
  }

};


#endif //BIGDATA_RACE_RIGHTTABLE_H
