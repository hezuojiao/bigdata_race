//
// Created by hezuojiao on 2019-10-22.
//

#include "Tables.h"

void Order::buildCache(const char* c_filename, const char* o_filename, bool rebuild) {
  o_orderkey  = (uint32_t***)malloc(sizeof(uint32_t**) * MAX_KEYS_NUM);
  o_orderdate = (uint16_t***)malloc(sizeof(uint16_t**) * MAX_KEYS_NUM);
  if (rebuild) {
    for (int idx = 0; idx < MAX_KEYS_NUM; ++idx) {
      o_orderkey[idx] = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
      o_orderdate[idx] = (uint16_t**)malloc(sizeof(uint16_t*) * MAX_CORE_NUM);
      for (int i = 0; i < MAX_CORE_NUM; ++i) {
        auto fd = open((O_ORDERKEY_PATH + util::IntToChar(i + idx * MAX_CORE_NUM)).c_str(), O_RDWR | O_CREAT, 0777);
        fallocate(fd, 0, 0, ORDER * sizeof(uint32_t));
        o_orderkey[idx][i] = (uint32_t *) mmap(nullptr, ORDER * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);

        fd = open((O_ORDERDATE_PATH + util::IntToChar(i + idx * MAX_CORE_NUM)).c_str(), O_RDWR | O_CREAT, 0777);
        fallocate(fd, 0, 0, ORDER * sizeof(uint16_t));
        o_orderdate[idx][i] = (uint16_t *) mmap(nullptr, ORDER * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        close(fd);
      }
    }
    parseColumns(c_filename, o_filename);
  } else {
    for (int idx = 0; idx < MAX_KEYS_NUM; ++idx) {
      o_orderkey[idx] = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
      o_orderdate[idx] = (uint16_t**)malloc(sizeof(uint16_t*) * MAX_CORE_NUM);
      for (int i = 0; i < MAX_CORE_NUM; ++i) {
        auto fd = open((O_ORDERKEY_PATH + util::IntToChar(i + idx * MAX_CORE_NUM)).c_str(), O_RDONLY, 0777);
        o_orderkey[idx][i] = (uint32_t *) mmap(nullptr, ORDER * sizeof(uint32_t), PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);

        fd = open((O_ORDERDATE_PATH + util::IntToChar(i + idx * MAX_CORE_NUM)).c_str(), O_RDONLY, 0777);
        o_orderdate[idx][i] = (uint16_t *) mmap(nullptr, ORDER * sizeof(uint16_t), PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
      }
    }
  }
}

void Order::parseColumns(const char *c_filename, const char *o_filename) {

  auto fd = open(c_filename, O_RDONLY , 0777);
  char* c_hashtable = (char*) malloc((CUSTOMER + 1) * sizeof(char));
  size_t len = util::file_size(c_filename), pos = 0;
  uint32_t c_custkey = 0;
  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  while (pos < len) {
    while (base[pos++] != '|') {}
    char name = base[pos++];
    while (pos < len && base[pos++] != '\n'){}
    c_hashtable[++c_custkey] = name;
  }

  munmap(base, len);
  close(fd);

  fd = open(o_filename, O_RDONLY, 0777);
  len = util::file_size(o_filename), pos = 0;
  uint32_t idx[MAX_KEYS_NUM][MAX_CORE_NUM] = { 0 };
  base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  uint32_t orderKey, custKey;
  uint16_t orderDate;

  while (pos < len) {
    orderKey = 0;
    while (base[pos] != '|') {
      orderKey = orderKey * 10 + base[pos] - '0';
      ++pos;
    }
    ++pos;
    custKey = 0;
    while (base[pos] != '|') {
      custKey = custKey * 10 + base[pos] - '0';
      ++pos;
    }
    orderDate = util::DateToInt(base + pos + 1);
    pos += 12;
    auto q = util::get_key_index(c_hashtable[custKey]);
    auto p = util::partition(orderKey);
    auto i = ++idx[q][p];
    o_orderkey[q][p][i] = orderKey;
    o_orderdate[q][p][i] = orderDate;
  }

  for (int i = 0; i < MAX_KEYS_NUM; ++i)
    for (int j = 0; j < MAX_CORE_NUM; ++j) {
      o_orderkey[i][j][0] = idx[i][j];
    }

  free(c_hashtable);
  munmap(base, len);
  close(fd);
}


void Lineitem::buildCache(const char *fileName, bool rebuild) {
  l_orderkey      = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  l_extendedprice = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  l_shipdate      = (uint16_t**)malloc(sizeof(uint16_t*) * MAX_CORE_NUM);
  if (rebuild) {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      auto fd = open((L_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(uint32_t));
      l_orderkey[i] = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((L_EXTENDEDPRICE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(uint32_t));
      l_extendedprice[i] = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((L_SHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, LINEITEM * sizeof(uint16_t));
      l_shipdate[i] = (uint16_t *) mmap(nullptr, LINEITEM * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);
    }

    parseColumns(fileName);
  } else {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      auto fd = open((L_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_orderkey[i] = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((L_EXTENDEDPRICE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_extendedprice[i] = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((L_SHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_shipdate[i] = (uint16_t *) mmap(nullptr, LINEITEM * sizeof(uint16_t), PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);
    }

  }
}

void Lineitem::parseColumns(const char *fileName) {
  auto fd = open(fileName, O_RDONLY, 0777);
  size_t len = util::file_size(fileName), pos = 0;
  uint32_t idx[MAX_CORE_NUM] = { 0 };
  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  uint32_t orderKey, extendedPrice;
  uint16_t shipDate;
  while (pos < len) {
    orderKey = 0;
    while (base[pos] != '|') {
      orderKey = orderKey * 10 + base[pos++] - '0';
    }
    ++pos;
    extendedPrice = 0;
    while (base[pos] != '|') {
      if (base[pos] != '.')
        extendedPrice = extendedPrice * 10 + base[pos] - '0';
      ++pos;
    }
    shipDate = util::DateToInt(base + pos + 1);
    pos += 12;
    auto p = util::partition(orderKey);
    auto i = ++idx[p];
    l_orderkey[p][i] = orderKey;
    l_extendedprice[p][i] = extendedPrice;
    l_shipdate[p][i] = shipDate;
  }

  for (int i = 0; i < MAX_CORE_NUM; ++i) {
    l_orderkey[i][0] = idx[i];
  }
  munmap(base, len);
  close(fd);
}