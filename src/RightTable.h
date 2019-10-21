//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_RIGHTTABLE_H
#define BIGDATA_RACE_RIGHTTABLE_H


#include "FileUtil.h"
#include "Constants.h"

using spp::sparse_hash_map;

class RightTable {
 private:
  int* l_orderkey;
  int* l_shipdate;
  int* l_extendedprice;
  int tablePosition;

 public:
  RightTable():tablePosition(0) {}


  ~RightTable() {
    size_t len = sizeof(int) * LINEITEM;
    munmap(l_orderkey, len);
    munmap(l_shipdate, len);
    munmap(l_extendedprice, len);
  }

  void buildCache(const char* lineitemFileName) {
    if (!file_exists("l_orderkey.cache")) {
      auto fd = open("l_orderkey.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(int));
      l_orderkey = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open("l_shipdate.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(int));
      l_shipdate = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open("l_extendedprice.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(int));
      l_extendedprice = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      parse(lineitemFileName);
    } else {
      auto fd = open("l_orderkey.cache", O_RDONLY, 0777);
      l_orderkey = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open("l_shipdate.cache", O_RDONLY, 0777);
      l_shipdate = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open("l_extendedprice.cache", O_RDONLY, 0777);
      l_extendedprice = (int*)mmap(nullptr, LINEITEM * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      tablePosition = LINEITEM;
    }
  }


  void parse(const char* lineitemFileName) {
    auto fd = open(lineitemFileName, O_RDONLY, 0777);
    size_t len = file_size(lineitemFileName), pos = 0;
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
        if (base[pos] != '.')
          num2 = num2 * 10 + base[pos] - '0';
        ++pos;
      }
      ++pos;
      num3 = 0;
      while (pos < len && base[pos] != '\n') {
        if (base[pos] != '-')
          num3 = num3 * 10 + base[pos] - '0';
        ++pos;
      }
      ++pos;
      addRow(num1, num2, num3);
    }
    munmap(base, len);
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
