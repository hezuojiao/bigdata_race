//
// Created by hezuojiao on 2019-11-14.
//

#ifndef BIGDATA_RACE_SORTEPS_H
#define BIGDATA_RACE_SORTEPS_H

#include <future>
#include "Constants.h"

class SortEps {

 private:
  uint32_t* eps;
  uint32_t* ok;
  uint32_t* lp;
  uint16_t* od;
  uint16_t* misd;
  uint16_t* masd;
  uint32_t size;

 public:
  SortEps(uint32_t* eps, uint32_t* ok, uint32_t* lp, uint16_t* od, uint16_t* misd, uint16_t* masd) :
  eps(eps), ok(ok), lp(lp), od(od), misd(misd) , masd(masd) {
    this->size = ok[0];  // array size stored in position 0.
  }

  void quicksort() {
    if (size > 0) {
      quicksort(1, size); // [1, size]
    }
  }

 private:
  void quicksort(uint32_t low, uint32_t high) {
    auto sz = high - low + 1;
    if (sz > MAX_LENGTH_INSERT_SORT) {
      uint32_t pivotLoc = partition(low, high);

      if (sz > MAX_LENGTH_QUICKSORT_ASYNC) {
        auto left = std::thread([&] { quicksort(low, pivotLoc - 1);});
        quicksort(pivotLoc + 1, high);
        left.join();
      } else {
        quicksort(low, pivotLoc - 1);
        quicksort(pivotLoc + 1, high);
      }
    } else {
      insertSort(low, high);
    }
  }

  uint32_t partition(uint32_t low, uint32_t high) {
    medianOfThree(low, high);
    uint32_t pivotKey = eps[low];
    uint32_t pivotOk = ok[low];
    uint32_t pivotLp = lp[low];
    uint16_t pivotOd = od[low];
    uint16_t pivotMisd = misd[low];
    uint16_t pivotMasd = masd[low];

    while (low < high) {
      while (low < high && pivotKey >= eps[high]) {
        high--;
      }
      store(low, high);

      while (low < high && eps[low] >= pivotKey) {
        low++;
      }
      store(high, low);
    }

    eps[low] = pivotKey;
    ok[low] = pivotOk;
    lp[low] = pivotLp;
    od[low] = pivotOd;
    misd[low] = pivotMisd;
    masd[low] = pivotMasd;

    return low;
  }

  void insertSort(uint32_t low, uint32_t high) {
    uint32_t i, j;
    for (i = low + 1; i <= high; i++) {
      if (eps[i] > eps[i - 1]) {
        uint32_t tmpKey = eps[i];
        uint32_t tmpOk = ok[i];
        uint32_t tmpLp = lp[i];
        uint16_t tmpOd = od[i];
        uint16_t tmpMisd = misd[i];
        uint16_t tmpMasd = masd[i];

        for (j = i - 1; j >= low && tmpKey > eps[j]; j--) {
          store(j + 1, j);
        }
        eps[j + 1] = tmpKey;
        ok[j + 1] = tmpOk;
        lp[j + 1] = tmpLp;
        od[j + 1] = tmpOd;
        misd[j + 1] = tmpMisd;
        masd[j + 1] = tmpMasd;
      }
    }
  }

  inline void medianOfThree(uint32_t low, uint32_t high) {
    uint32_t mid = ((low + high) >> 1);

    if (eps[high] > eps[mid])
      swap(mid, high);

    if (eps[mid] > eps[low])
      swap(low, mid);
  }

  inline void store(uint32_t dst, uint32_t src) {
    if (dst == src) return;
    eps[dst] = eps[src];
    ok[dst] = ok[src];
    lp[dst] = lp[src];
    od[dst] = od[src];
    misd[dst] = misd[src];
    masd[dst] = masd[dst];
  }

  inline void swap(uint32_t left, uint32_t right) {
    if (left == right) return;
    eps[left] ^= eps[right] ^= eps[left] ^= eps[right];
    ok[left] ^= ok[right] ^= ok[left] ^= ok[right];
    lp[left] ^= lp[right] ^= lp[left] ^= lp[right];
    od[left] ^= od[right] ^= od[left] ^= od[right];
    misd[left] ^= misd[right] ^= misd[left] ^= misd[right];
    masd[left] ^= masd[right] ^= masd[left] ^= masd[right];
  }
};

#endif //BIGDATA_RACE_SORTEPS_H
