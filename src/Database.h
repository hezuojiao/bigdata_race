//
// Created by hezuojiao on 2019-11-13.
//

#ifndef BIGDATA_RACE_DATABASE_H
#define BIGDATA_RACE_DATABASE_H

#include "Utils.h"
#include "Constants.h"
#include "SortEps.h"

class Database {

 private:
  // lineitem table
  uint32_t* l_orderkey;
  uint32_t* l_extendedprice;
  uint16_t* l_shipdate;

  // aggregation table
  uint16_t* a_orderdate[MAX_KEYS_NUM];
  uint16_t* a_minshipdate[MAX_KEYS_NUM];
  uint16_t* a_maxshipdate[MAX_KEYS_NUM];
  uint32_t* a_extendedpricesum[MAX_KEYS_NUM];
  uint32_t* a_orderkey[MAX_KEYS_NUM];
  uint32_t* a_lineitemposition[MAX_KEYS_NUM];

 public:

  Database() {}

  ~Database() {
//    munmap
  }

  void importDB(const char* c_filename, const char* o_filename, const char* l_filename);

  inline char* query(char mktsegmentCondition, uint16_t orderdateCondition, uint16_t shipdateCondition, uint16_t topn) {
    auto p = util::get_key_index(mktsegmentCondition);
    auto A_LENGTH = a_orderkey[p][0];
    MaxHeap heap(topn);
    for (uint32_t pos = 1; pos <= A_LENGTH; ++pos) {
      if (a_extendedpricesum[p][pos] <= heap.top()) {  // early stop
        break;
      }
      if (a_orderdate[p][pos] < orderdateCondition) {
        if (a_minshipdate[p][pos] > shipdateCondition) {
          heap.insert(a_orderkey[p][pos], a_orderdate[p][pos], a_extendedpricesum[p][pos]);
        } else if (a_maxshipdate[p][pos] > shipdateCondition) { // lookup lineitem table
          uint32_t l_position = a_lineitemposition[p][pos];
          uint32_t joinkey = a_orderkey[p][pos];
          uint32_t eps = 0;
          while (l_orderkey[l_position] == joinkey) {
            if (l_shipdate[l_position] > shipdateCondition) {
              eps += l_extendedprice[l_position];
            }
            ++l_position;
          }
          heap.insert(a_orderkey[p][pos], a_orderdate[p][pos], eps);
        }
      }
    }
    return heap.getResults();
  }

 private:

  void flush();

  struct MaxHeap {
    uint32_t* c1Result;
    uint16_t* c2Result;
    uint32_t* c3Result;
    uint16_t  size;

    MaxHeap(uint16_t size) : size(size) {
      this->c1Result = new uint32_t[size];
      this->c2Result = new uint16_t[size];
      this->c3Result = new uint32_t[size];
      memset(c3Result, 0, sizeof(uint32_t) * size);
    }

    ~MaxHeap() {
      delete[] c1Result;
      delete[] c2Result;
      delete[] c3Result;
    }

    inline uint32_t top() const {
      return c3Result[0];
    }

    inline void insert(uint32_t orderkey, uint16_t orderdate, uint32_t extendedpricesum) {
      if (extendedpricesum <= c3Result[0]) {
        return;
      }

      c1Result[0] = orderkey;
      c2Result[0] = orderdate;
      c3Result[0] = extendedpricesum;
      adjust(size);
    }

    inline void adjust(int end) {
      int i = 0, c;
      while (i < end) {
        c = i * 2 + 1;
        if (c >= end) break;
        if (c + 1 < end && c3Result[c] > c3Result[c + 1]) ++c;
        if (c3Result[i] > c3Result[c]) {
          swap(i, c);
          i = c;
        } else {
          break;
        }
      }
    }

    char* getResults() {
      for (int i = size - 1; i > 0; i--) {
        swap(0, i);
        adjust(i);
      }

      auto char_buf = new char[40 * (size + 1)];
      int pos = 0;
      pos += sprintf(char_buf, "l_orderkey|o_orderdate|revenue\n");
      for (int i = 0; i < size; i++) {
        if (c3Result[i] > 0) {
          pos += sprintf(char_buf + pos,
                         "%u|199%d-%02d-%02d|%.2f\n", c1Result[i],
                         c2Result[i] / 10000 + 2, (c2Result[i] % 10000) / 100, c2Result[i] % 100,
                         (double) c3Result[i] / 100.0);
        } else {
          break;
        }
      }
      return char_buf;
    }

    inline void swap(int left, int right) {
      c1Result[left] ^= c1Result[right] ^= c1Result[left] ^= c1Result[right];
      c2Result[left] ^= c2Result[right] ^= c2Result[left] ^= c2Result[right];
      c3Result[left] ^= c3Result[right] ^= c3Result[left] ^= c3Result[right];
    }
  };
};

void Database::importDB(const char *c_filename, const char *o_filename, const char *l_filename) {

  auto exist = util::file_exists(L_ORDERKEY_PATH.c_str());

  if (exist) { // mmap file
    auto l_orderkey_fd = open(L_ORDERKEY_PATH.c_str(), O_RDONLY, 0777);
    auto l_extendedprice_fd = open(L_EXTENDEDPRICE_PATH.c_str(), O_RDONLY, 0777);
    auto l_shipdate_fd = open(L_SHIPDATE_PATH.c_str(), O_RDONLY, 0777);

    l_orderkey = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ, MAP_PRIVATE, l_orderkey_fd, 0);
    l_extendedprice = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ, MAP_PRIVATE, l_extendedprice_fd, 0);
    l_shipdate = (uint16_t *) mmap(nullptr, LINEITEM * sizeof(uint16_t), PROT_READ, MAP_PRIVATE, l_shipdate_fd, 0);

    close(l_orderkey_fd);
    close(l_extendedprice_fd);
    close(l_shipdate_fd);


    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      auto a_orderdate_fd = open((A_ORDERDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      auto a_minshipdate_fd = open((A_MINSHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      auto a_maxshipdate_fd = open((A_MAXSHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      auto a_extendedpricesum_fd = open((A_EXTENDEDPRICESUM_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      auto a_orderkey_fd = open((A_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);
      auto a_lineitemposition_fd = open((A_LINEITEMPOSITION_PATH + util::IntToChar(i)).c_str(), O_RDONLY, 0777);

      auto size = util::file_size((A_ORDERDATE_PATH + util::IntToChar(i)).c_str());

      a_orderdate[i] = (uint16_t*) mmap(nullptr, size, PROT_READ, MAP_PRIVATE, a_orderdate_fd, 0);
      a_minshipdate[i] = (uint16_t*) mmap(nullptr, size, PROT_READ, MAP_PRIVATE, a_minshipdate_fd, 0);
      a_maxshipdate[i] = (uint16_t*) mmap(nullptr, size, PROT_READ, MAP_PRIVATE, a_maxshipdate_fd, 0);
      a_extendedpricesum[i] = (uint32_t *) mmap(nullptr, size * 2, PROT_READ, MAP_PRIVATE, a_extendedpricesum_fd, 0);
      a_orderkey[i] = (uint32_t*) mmap(nullptr, size * 2, PROT_READ, MAP_PRIVATE, a_orderkey_fd, 0);
      a_lineitemposition[i] = (uint32_t*) mmap(nullptr, size * 2, PROT_READ, MAP_PRIVATE, a_lineitemposition_fd, 0);

      close(a_orderdate_fd);
      close(a_minshipdate_fd);
      close(a_maxshipdate_fd);
      close(a_extendedpricesum_fd);
      close(a_orderkey_fd);
      close(a_lineitemposition_fd);
    }

  } else { // read and process data

    auto l_orderkey_fd = open(L_ORDERKEY_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    auto l_extendedprice_fd = open(L_EXTENDEDPRICE_PATH.c_str(), O_RDWR | O_CREAT, 0777);
    auto l_shipdate_fd = open(L_SHIPDATE_PATH.c_str(), O_RDWR | O_CREAT, 0777);

    fallocate(l_orderkey_fd, 0, 0, LINEITEM * sizeof(uint32_t));
    fallocate(l_extendedprice_fd, 0, 0, LINEITEM * sizeof(uint32_t));
    fallocate(l_shipdate_fd, 0, 0, LINEITEM * sizeof(uint16_t));

    l_orderkey = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, l_orderkey_fd, 0);
    l_extendedprice = (uint32_t *) mmap(nullptr, LINEITEM * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, l_extendedprice_fd, 0);
    l_shipdate = (uint16_t *) mmap(nullptr, LINEITEM * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, l_shipdate_fd, 0);

    close(l_orderkey_fd);
    close(l_extendedprice_fd);
    close(l_shipdate_fd);

    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      a_orderdate[i] = (uint16_t*) malloc(ORDER * sizeof(uint16_t));
      a_minshipdate[i] = (uint16_t*) malloc(ORDER * sizeof(uint16_t));
      a_maxshipdate[i] = (uint16_t*) malloc(ORDER * sizeof(uint16_t));
      a_extendedpricesum[i] = (uint32_t *) malloc(ORDER * sizeof(uint32_t));
      a_orderkey[i] = (uint32_t*) malloc(ORDER * sizeof(uint32_t));
      a_lineitemposition[i] = (uint32_t*) malloc(ORDER * sizeof(uint32_t));
    }

    // process lineitem.txt
    auto l_fd = open(l_filename, O_RDONLY, 0777);
    size_t len = util::file_size(l_filename), pos = 0;
    char* base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, l_fd, 0);
    posix_fadvise(l_fd, 0, len, POSIX_FADV_WILLNEED);
    uint32_t orderKey, extendedPrice, index = 0;
    uint16_t date;
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
      date = util::DateToInt(base + pos + 1);
      pos += 12;
      l_orderkey[index] = orderKey;
      l_extendedprice[index] = extendedPrice;
      l_shipdate[index++] = date;
    }

    munmap(base, len);
    close(l_fd);

    // process customer.txt
    auto c_fd = open(c_filename, O_RDONLY , 0777);
    char* c_data = (char*) malloc((CUSTOMER + 1) * sizeof(char));
    len = util::file_size(c_filename), pos = 0;
    uint32_t custkey = 0;
    base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, c_fd, 0);
    posix_fadvise(c_fd, 0, len, POSIX_FADV_WILLNEED);
    while (pos < len) {
      while (base[pos++] != '|') {}
      char name = base[pos++];
      while (pos < len && base[pos++] != '\n'){}
      c_data[++custkey] = name;
    }

    munmap(base, len);
    close(c_fd);


    // process orders.txt
    auto o_fd = open(o_filename, O_RDONLY, 0777);
    len = util::file_size(o_filename), pos = 0;
    base = (char*)mmap(nullptr, len, PROT_READ, MAP_SHARED, o_fd, 0);
    posix_fadvise(o_fd, 0, len, POSIX_FADV_WILLNEED);

    uint32_t idxes[MAX_KEYS_NUM] = { 0 }, l_position = 0;
    uint16_t min_shipdate, max_shipdate;
    uint32_t eps;

    while (pos < len) {
      // read data
      orderKey = 0;
      while (base[pos] != '|') {
        orderKey = orderKey * 10 + base[pos] - '0';
        ++pos;
      }
      ++pos;
      custkey = 0;
      while (base[pos] != '|') {
        custkey = custkey * 10 + base[pos] - '0';
        ++pos;
      }
      date = util::DateToInt(base + pos + 1);
      pos += 12;

      // find lineitem position
      if (orderKey != l_orderkey[l_position]) {
        printf("ERROR   HAHAHAHAHAHHA\n");
      }

      auto i = util::get_key_index(c_data[custkey]);
      auto idx = ++idxes[i];
      a_orderdate[i][idx] = date;
      a_orderkey[i][idx] = orderKey;
      a_lineitemposition[i][idx] = l_position;
      min_shipdate = max_shipdate = l_shipdate[l_position];
      eps = l_extendedprice[l_position++];
      while (orderKey == l_orderkey[l_position]) {
        if (l_shipdate[l_position] < min_shipdate) min_shipdate = l_shipdate[l_position];
        if (l_shipdate[l_position] > max_shipdate) max_shipdate = l_shipdate[l_position];
        eps += l_extendedprice[l_position++];
      }

      a_minshipdate[i][idx] = min_shipdate;
      a_maxshipdate[i][idx] = max_shipdate;
      a_extendedpricesum[i][idx] = eps;
    }

    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      a_orderkey[i][0] = idxes[i];
    }

    free(c_data);
    munmap(base, len);
    close(o_fd);

    // sort
    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      SortEps sortEps(a_extendedpricesum[i], a_orderkey[i], a_lineitemposition[i], a_orderdate[i], a_minshipdate[i], a_maxshipdate[i]);
      sortEps.quicksort();
    }

    // flush
    flush();
  }
}

void Database::flush() {
  for (int i = 0; i < MAX_KEYS_NUM; ++i) {
    auto a_orderdate_fd = open((A_ORDERDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
    auto a_minshipdate_fd = open((A_MINSHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
    auto a_maxshipdate_fd = open((A_MAXSHIPDATE_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
    auto a_extendedpricesum_fd = open((A_EXTENDEDPRICESUM_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
    auto a_orderkey_fd = open((A_ORDERKEY_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);
    auto a_lineitemposition_fd = open((A_LINEITEMPOSITION_PATH + util::IntToChar(i)).c_str(), O_RDWR | O_CREAT, 0777);

    auto size = a_orderkey[i][0] + 1;

    fallocate(a_orderdate_fd, 0, 0, size * sizeof(uint16_t));
    fallocate(a_minshipdate_fd, 0, 0, size * sizeof(uint16_t));
    fallocate(a_maxshipdate_fd, 0, 0, size * sizeof(uint16_t));
    fallocate(a_extendedpricesum_fd, 0, 0, size * sizeof(uint32_t));
    fallocate(a_orderkey_fd, 0, 0, size * sizeof(uint32_t));
    fallocate(a_lineitemposition_fd, 0, 0, size * sizeof(uint32_t));

    auto a_orderdate_mmap = (uint16_t*) mmap(nullptr, size * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_orderdate_fd, 0);
    auto a_minshipdate_mmap = (uint16_t*) mmap(nullptr, size * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_minshipdate_fd, 0);
    auto a_maxshipdate_mmap = (uint16_t*) mmap(nullptr, size * sizeof(uint16_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_maxshipdate_fd, 0);
    auto a_extendedpricesum_mmap = (uint32_t *) mmap(nullptr, size * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_extendedpricesum_fd, 0);
    auto a_orderkey_mmap = (uint32_t*) mmap(nullptr, size * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_orderkey_fd, 0);
    auto a_lineitemposition_mmap = (uint32_t*) mmap(nullptr, size * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_SHARED, a_lineitemposition_fd, 0);

    memcpy(a_orderdate_mmap, a_orderdate[i], size * sizeof(uint16_t));
    memcpy(a_minshipdate_mmap, a_minshipdate[i], size * sizeof(uint16_t));
    memcpy(a_maxshipdate_mmap, a_maxshipdate[i], size * sizeof(uint16_t));
    memcpy(a_extendedpricesum_mmap, a_extendedpricesum[i], size * sizeof(uint32_t));
    memcpy(a_orderkey_mmap, a_orderkey[i], size * sizeof(uint32_t));
    memcpy(a_lineitemposition_mmap, a_lineitemposition[i], size * sizeof(uint32_t));

    close(a_orderdate_fd);
    close(a_minshipdate_fd);
    close(a_maxshipdate_fd);
    close(a_extendedpricesum_fd);
    close(a_orderkey_fd);
    close(a_lineitemposition_fd);
  }
}


#endif //BIGDATA_RACE_DATABASE_H
