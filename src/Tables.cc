//
// Created by hezuojiao on 2019-10-22.
//

#include "Tables.h"


void Customer::buildCache(const char *fileName, bool rebuild) {
  if (rebuild) {
    auto fd = open(C_MKTSEMENT_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, CUSTOMER_FILE_SIZE);
    c_mktsegment = (char*)mmap(nullptr, CUSTOMER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    parseColumns(fileName);
  } else {
    auto fd = open(C_MKTSEMENT_PATH.c_str(), O_RDONLY, 0777);
    c_mktsegment = (char*)mmap(nullptr, CUSTOMER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
  }

  for (int i = 0; i < CUSTOMER; i++) {
    c_hashtable[c_mktsegment[i]].insert(i + 1);
  }

  munmap(c_mktsegment, CUSTOMER_FILE_SIZE);
}

void Customer::parseColumns(const char *fileName) {
  auto fd = open(fileName, O_RDONLY , 0777);

  size_t len = util::file_size(fileName), pos = 0;

  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  while (pos < len) {
    while (base[pos++] != '|') {}
    char name = base[pos++];
    while (pos < len && base[pos++] != '\n'){}
    c_mktsegment[position++] = name;
  }
  munmap(base, len);
  close(fd);
}

void Order::buildCache(const char *fileName, bool rebuild) {
  if (rebuild) {
    auto fd = open(O_ORDERKEY_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, ORDER_FILE_SIZE);
    o_orderkey = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    fd = open(O_CUSTKEY_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, ORDER_FILE_SIZE);
    o_custkey = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    fd = open(O_ORDERDATE_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, ORDER_FILE_SIZE);
    o_orderdate = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    parseColumns(fileName);
  } else {
    auto fd = open(O_ORDERKEY_PATH.c_str(), O_RDONLY, 0777);
    o_orderkey = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    fd = open(O_CUSTKEY_PATH.c_str(), O_RDONLY, 0777);
    o_custkey = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    fd = open(O_ORDERDATE_PATH.c_str(), O_RDONLY, 0777);
    o_orderdate = (int*)mmap(nullptr, ORDER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    position = ORDER;
  }
}

void Order::parseColumns(const char *fileName) {
  auto fd = open(fileName, O_RDONLY, 0777);
  size_t len = util::file_size(fileName), pos = 0;
  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  int num1, num2, num3;
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


void Lineitem::buildCache(const char *fileName, bool rebuild) {
  if (rebuild) {
    auto fd = open(L_ORDERKEY_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, LINEITEM_FILE_SIZE);
    l_orderkey = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    fd = open(L_SHIPDATE_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, LINEITEM_FILE_SIZE);
    l_shipdate = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    fd = open(L_EXTENDEDPRICE_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    fallocate(fd, 0, 0, LINEITEM_FILE_SIZE);
    l_extendedprice = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);

    parseColumns(fileName);
  } else {
    auto fd = open(L_ORDERKEY_PATH.c_str(), O_RDONLY, 0777);
    l_orderkey = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    fd = open(L_SHIPDATE_PATH.c_str(), O_RDONLY, 0777);
    l_shipdate = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    fd = open(L_EXTENDEDPRICE_PATH.c_str(), O_RDONLY, 0777);
    l_extendedprice = (int*)mmap(nullptr, LINEITEM_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    position = LINEITEM;
  }
}

void Lineitem::parseColumns(const char *fileName) {
  auto fd = open(fileName, O_RDONLY, 0777);
  size_t len = util::file_size(fileName), pos = 0;
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