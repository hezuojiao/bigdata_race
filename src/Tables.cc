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
  uint32_t c_custkey = 0;

  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
  while (pos < len) {
    while (base[pos++] != '|') {}
    char name = base[pos++];
    while (pos < len && base[pos++] != '\n'){}
    c_hashtable[name].insert(++c_custkey);
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
  c_hashtable.reserve(5);
  std::thread loader[5];
  for (int i = 0; i < 5; ++i) {
    loader[i] = std::thread([i, this]{
      auto load_file = C_PATH + C_KEYS[i];
      phmap::BinaryInputArchive ht_load(load_file.c_str());
      phmap::flat_hash_set<uint32_t> hs;
      hs.load(ht_load);
      c_hashtable[C_KEYS[i]] = hs;
    });
  }
  for (auto &t : loader) {
    t.join();
  }
}



void Order::buildCache(const char *fileName, bool rebuild) {
  o_orderkey  = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  o_custkey   = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  o_orderdate = (uint16_t**)malloc(sizeof(uint16_t*) * MAX_CORE_NUM);
  if (rebuild) {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      auto fd = open((O_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER_FILE_SIZE);
      o_orderkey[i] = (uint32_t *) mmap(nullptr, ORDER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((O_CUSTKEY_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER_FILE_SIZE);
      o_custkey[i] = (uint32_t *) mmap(nullptr, ORDER_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((O_ORDERDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, ORDER_FILE_SIZE);
      o_orderdate[i] = (uint16_t *) mmap(nullptr, ORDER_FILE_SIZE/2, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);
    }

    parseColumns(fileName);
  } else {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      auto fd = open((O_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      o_orderkey[i] = (uint32_t *) mmap(nullptr, ORDER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((O_CUSTKEY_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      o_custkey[i] = (uint32_t *) mmap(nullptr, ORDER_FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((O_ORDERDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      o_orderdate[i] = (uint16_t *) mmap(nullptr, ORDER_FILE_SIZE/2, PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);
    }
  }
}

void Order::parseColumns(const char *fileName) {
  auto fd = open(fileName, O_RDONLY, 0777);
  size_t len = util::file_size(fileName), pos = 0;
  uint32_t idx[MAX_CORE_NUM] = { 0 };
  char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
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
    auto p = util::partition(orderKey);
    auto i = idx[p]++;
    o_orderkey[p][i] = orderKey;
    o_custkey[p][i] = custKey;
    o_orderdate[p][i] = orderDate;
  }

  munmap(base, len);
  close(fd);
}


void Lineitem::buildCache(const char *fileName, bool rebuild) {
  l_orderkey      = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  l_extendedprice = (uint32_t**)malloc(sizeof(uint32_t*) * MAX_CORE_NUM);
  l_shipdate      = (uint16_t**)malloc(sizeof(uint16_t*) * MAX_CORE_NUM);
  if (rebuild) {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {

      size_t FILE_SIZE = LINEITEMS[i] * sizeof(uint32_t);

      auto fd = open((L_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, FILE_SIZE);
      l_orderkey[i] = (uint32_t *) mmap(nullptr, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((L_EXTENDEDPRICE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, FILE_SIZE);
      l_extendedprice[i] = (uint32_t *) mmap(nullptr, FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);

      fd = open((L_SHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
      fallocate(fd, 0, 0, FILE_SIZE);
      l_shipdate[i] = (uint16_t *) mmap(nullptr, FILE_SIZE/2, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      close(fd);
    }

    parseColumns(fileName);
  } else {
    for (int i = 0; i < MAX_CORE_NUM; ++i) {

      size_t FILE_SIZE = LINEITEMS[i] * sizeof(uint32_t);

      auto fd = open((L_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_orderkey[i] = (uint32_t *) mmap(nullptr, FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((L_EXTENDEDPRICE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_extendedprice[i] = (uint32_t *) mmap(nullptr, FILE_SIZE, PROT_READ, MAP_PRIVATE, fd, 0);
      close(fd);

      fd = open((L_SHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      l_shipdate[i] = (uint16_t *) mmap(nullptr, FILE_SIZE/2, PROT_READ, MAP_PRIVATE, fd, 0);
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
    auto i = idx[p]++;
    l_orderkey[p][i] = orderKey;
    l_extendedprice[p][i] = extendedPrice;
    l_shipdate[p][i] = shipDate;
  }
  munmap(base, len);
  close(fd);
}