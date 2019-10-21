//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_LEFTTABLE_H
#define BIGDATA_RACE_LEFTTABLE_H


#include "FileUtil.h"
#include "Constants.h"

using spp::sparse_hash_map;
using spp::sparse_hash_set;

class LeftTable {
 private:
  char* c_mktsegment;
  int* o_orderkey;
  int* o_custkey;
  int* o_orderdate;
  int tablePosition;

 private:
  sparse_hash_map<char, sparse_hash_set<int>> c_hashtable;

 public:
  LeftTable():tablePosition(0) {}


  ~LeftTable() {
    size_t len = sizeof(int) * ORDER;
    munmap(o_orderkey, len);
    munmap(o_custkey, len);
    munmap(o_orderdate, len);
  }


  void buildCache(const char* customerFileName, const char* orderFileName) {
    if (!file_exists("c_mktsegment.cache")) {
      auto fd = open("c_mktsegment.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, CUSTOMER * sizeof(char));
      c_mktsegment = (char*)mmap(nullptr, CUSTOMER * sizeof(char), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open("o_orderkey.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER * sizeof(int));
      o_orderkey = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open("o_custkey.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER * sizeof(int));
      o_custkey = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open("o_orderdate.cache", O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER * sizeof(int));
      o_orderdate = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      parse(customerFileName, orderFileName);
    } else {

      auto fd = open("c_mktsegment.cache", O_RDONLY, 0777);
      c_mktsegment = (char*)mmap(nullptr, CUSTOMER * sizeof(char), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open("o_orderkey.cache", O_RDONLY, 0777);
      o_orderkey = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open("o_custkey.cache", O_RDONLY, 0777);
      o_custkey = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open("o_orderdate.cache", O_RDONLY, 0777);
      o_orderdate = (int*)mmap(nullptr, ORDER * sizeof(int), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      tablePosition = ORDER;
    }

    for (int i = 0; i < CUSTOMER; i++) {
      c_hashtable[c_mktsegment[i]].insert(i + 1);
    }
    munmap(c_mktsegment, CUSTOMER * sizeof(char));
  }


  void parse(const char* customerFileName, const char* orderFileName) {
    auto fd = open(customerFileName, O_RDONLY , 0777);
    struct stat s;
    if (fstat(fd, &s) < 0) {
      close(fd);
      return;
    }

    size_t len = s.st_size, pos = 0;
    char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
    posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
    int num1 = 0, num2, num3;
    while (pos < len) {
      while (base[pos++] != '|') {}
      char name = base[pos++];
      while (pos < len && base[pos++] != '\n'){}
      c_mktsegment[num1++] = name;
    }

    munmap(base, s.st_size);
    close(fd);

    fd = open(orderFileName, O_RDONLY, 0777);
    if (fstat(fd, &s) < 0) {
      close(fd);
      return;
    }

    len = s.st_size, pos = 0;
    base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
    posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
    while (pos < len) {
      num1 = 0;
      while (base[pos] != '|') {
        num1 = num1 * 10 + base[pos] - '0';
        ++pos;
      }
      ++pos;
      num2 = 0;
      while (base[pos] != '|') {
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

  void filterAfterHashJoin(char mktsegmentCondition, int orderdateCondition,
      std::vector<int>& orderkey, std::vector<int>& orderdate) {
    auto c_custkey = c_hashtable[mktsegmentCondition];
    for (int i = 0; i < tablePosition; i++) {
      if (c_custkey.find(o_custkey[i]) != c_custkey.end() && o_orderdate[i] < orderdateCondition) {
        orderkey.push_back(o_orderkey[i]);
        orderdate.push_back(o_orderdate[i]);
      }
    }
  }

 private:
  void addRow(int orderKey, int custKey, int orderDate) {
    o_orderkey[tablePosition] = orderKey;
    o_custkey[tablePosition] = custKey;
    o_orderdate[tablePosition] = orderDate;
    tablePosition++;
  }
};

#endif //BIGDATA_RACE_LEFTTABLE_H
