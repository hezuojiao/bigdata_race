//
// Created by hezuojiao on 2019-10-22.
//

#include "Tables.h"


void Customer::buildCache(const char *fileName, bool rebuild) {
  if (rebuild) {
    parseColumns(fileName);
    serialize();
  } else {
    deserialize();
  }
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
    c_hashtable[name].insert(++position);
  }

  munmap(base, len);
  close(fd);
}

void Customer::serialize() {
  for (auto& hs : c_hashtable) {
    auto dump_file = C_PATH + hs.first;
    phmap::BinaryOutputArchive ht_dump(dump_file.c_str());
    hs.second.dump(ht_dump);
  }
}

void Customer::deserialize() {
  for (auto ch : C_KEYS) {
    auto load_file = C_PATH + ch;
    phmap::BinaryInputArchive ht_load(load_file.c_str());
    phmap::flat_hash_set<int> hs;
    hs.load(ht_load);
    c_hashtable[ch] = hs;
  }
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