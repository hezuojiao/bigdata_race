//
// Created by hezuojiao on 2019-11-13.
//

#ifndef BIGDATA_RACE_DATABASE_H
#define BIGDATA_RACE_DATABASE_H

#include <atomic>
#include <thread>

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

  std::atomic<uint8_t> pending;

 public:

  Database() : pending(0) {}

  ~Database() {
//    munmap

    // wait flush
    while (pending.load() != 0) {}
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

  void flush(int i);

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

    pending = MAX_KEYS_NUM;

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


    std::thread workers[MAX_CORE_NUM];

    auto l_fd = open(l_filename, O_RDONLY, 0777);
    auto c_fd = open(c_filename, O_RDONLY, 0777);

    size_t l_len = util::file_size(l_filename) ;
    size_t c_len = util::file_size(c_filename);

    char* l_base = (char*)mmap(nullptr, l_len, PROT_READ, MAP_PRIVATE, l_fd, 0);
    char* c_base = (char*)mmap(nullptr, c_len, PROT_READ, MAP_PRIVATE, c_fd, 0);
    char* c_data = (char*) malloc((CUSTOMER + 1) * sizeof(char));

    madvise(l_base, l_len, MADV_SEQUENTIAL);
    madvise(c_base, c_len, MADV_SEQUENTIAL);


    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      workers[i] = std::thread([&, i] {

        // process lineitem.txt
        uint32_t mmap_position;
        size_t thread_st, thread_ed;
        mmap_position = L_POSISTIONS[i];
        thread_st = L_OFFSETS[i];
        thread_ed = (i == MAX_CORE_NUM - 1) ? l_len : L_OFFSETS[i + 1];

        uint32_t thread_ok, thread_ep;
        uint16_t thread_sd;

        while (thread_st < thread_ed) {
          thread_ok = 0;
          while (l_base[thread_st] != '|') {
            thread_ok = thread_ok * 10 + l_base[thread_st++] - '0';
          }
          ++thread_st;
          thread_ep = 0;
          while (l_base[thread_st] != '|') {
            if (l_base[thread_st] != '.')
              thread_ep = thread_ep * 10 + l_base[thread_st] - '0';
            ++thread_st;
          }
          thread_sd = util::DateToInt(l_base + thread_st + 1);
          thread_st += 12;
          l_orderkey[mmap_position] = thread_ok;
          l_extendedprice[mmap_position] = thread_ep;
          l_shipdate[mmap_position++] = thread_sd;
        }

        // process customer.txt
        thread_st = C_OFFSETS[i];
        thread_ed = (i == MAX_CORE_NUM - 1) ? c_len : C_OFFSETS[i + 1];

        uint32_t thread_ck = 0;
        char thread_name;
        while (c_base[thread_st] != '|') { // read first custkey.
          thread_ck = thread_ck * 10 + c_base[thread_st++] - '0';
        }
        thread_name = c_base[thread_st + 1];
        c_data[thread_ck++] = thread_name;
        while (c_base[thread_st++] != '\n') {}

        while (thread_st < thread_ed) {
          while (c_base[thread_st++] != '|') {}
          thread_name = c_base[thread_st++];
          while (c_base[thread_st++] != '\n') {}
          c_data[thread_ck++] = thread_name;
        }
      });
    }

    for (auto &t : workers) {
      t.join();
    }
    munmap(l_base, l_len);
    munmap(c_base, c_len);
    close(l_fd);
    close(c_fd);


    // process orders.txt
    auto o_fd = open(o_filename, O_RDONLY, 0777);
    size_t o_len = util::file_size(o_filename);
    char* o_base = (char*)mmap(nullptr, o_len, PROT_READ, MAP_PRIVATE, o_fd, 0);
    madvise(o_base, o_len, MADV_SEQUENTIAL);

    std::atomic<uint32_t> idxes[MAX_KEYS_NUM] = {};

    for (int i = 0; i < MAX_CORE_NUM; ++i) {
      workers[i] = std::thread([&, i] {
        size_t thread_st = O_OFFSETS[i];
        size_t thread_ed = (i == MAX_CORE_NUM - 1) ? o_len : O_OFFSETS[i + 1];

        uint32_t thread_ok = 0, thread_ck, thread_idx, thread_eps;
        uint16_t thread_od, thread_misd, thread_masd;

        // read first orderkey.
        while (o_base[thread_st] != '|') {
          thread_ok = thread_ok * 10 + o_base[thread_st++] - '0';
        }

//        uint32_t thread_lpos = util::binary_search(l_orderkey, LINEITEM, thread_ok);
        uint32_t thread_lpos = L_POS[i];
        thread_st = O_OFFSETS[i]; // reset read position

        while (thread_st < thread_ed) {
          thread_ok = 0;
          while (o_base[thread_st] != '|') {
            thread_ok = thread_ok * 10 + o_base[thread_st++] - '0';
          }
          ++thread_st;
          thread_ck = 0;
          while (o_base[thread_st] != '|') {
            thread_ck = thread_ck * 10 + o_base[thread_st++] - '0';
          }

          thread_od = util::DateToInt(o_base + thread_st + 1);
          thread_st += 12;

          auto thread_pt = util::get_key_index(c_data[thread_ck]);
          thread_idx = ++idxes[thread_pt];
          a_orderdate[thread_pt][thread_idx] = thread_od;
          a_orderkey[thread_pt][thread_idx] = thread_ok;
          a_lineitemposition[thread_pt][thread_idx] = thread_lpos;
          thread_misd = thread_masd = l_shipdate[thread_lpos];
          thread_eps = l_extendedprice[thread_lpos++];
          while (thread_ok == l_orderkey[thread_lpos]) {
            if (l_shipdate[thread_lpos] < thread_misd) thread_misd = l_shipdate[thread_lpos];
            if (l_shipdate[thread_lpos] > thread_masd) thread_masd = l_shipdate[thread_lpos];
            thread_eps += l_extendedprice[thread_lpos++];
          }
          a_minshipdate[thread_pt][thread_idx] = thread_misd;
          a_maxshipdate[thread_pt][thread_idx] = thread_masd;
          a_extendedpricesum[thread_pt][thread_idx] = thread_eps;
        }
      });
    }

    for (auto &t : workers) {
      t.join();
    }
    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      a_orderkey[i][0] = idxes[i];
    }

    free(c_data);
    munmap(c_base, c_len);
    close(o_fd);

    std::thread sorter[MAX_KEYS_NUM];
    // sort
    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      sorter[i] = std::thread([i, this] {
        SortEps sortEps(a_extendedpricesum[i], a_orderkey[i], a_lineitemposition[i], a_orderdate[i], a_minshipdate[i], a_maxshipdate[i]);
        sortEps.quicksort();
      });
    }

    for (int i = 0; i < MAX_KEYS_NUM; ++i) {
      sorter[i].join();
      std::thread([i, this] { flush(i); }).detach();
    }
  }
}

void Database::flush(int i) {

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

  --pending;
}


#endif //BIGDATA_RACE_DATABASE_H
