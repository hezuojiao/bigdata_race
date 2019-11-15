//
// Created by hezuojiao on 2019-10-21.
//

#ifndef BIGDATA_RACE_UTILS_H
#define BIGDATA_RACE_UTILS_H


#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "Constants.h"

namespace util {

static inline bool file_exists(const char *file_name) {
  struct stat st;
  return (stat(file_name, &st) == 0);
}

static inline size_t file_size(const char *file_name) {
  struct stat st;
  stat(file_name, &st);
  size_t size = st.st_size;
  return size;
}

static inline uint16_t DateToInt(const char* date) {
  return (date[3] - '2') * 10000 + (date[5] - '0') * 1000 + (date[6] - '0') * 100 + (date[8] - '0') * 10 + date[9] - '0';
}

static inline char IntToChar(int i) {
  return i + '0';
}

static inline int get_key_index(const char key) {
  for (int i = 0; i < MAX_KEYS_NUM; ++i) if (keys[i] == key) return i;
}

static inline uint32_t binary_search(const uint32_t* array, uint32_t size, uint32_t target) {
  uint32_t left = 0, right = size, middle;
  while (left < right) {
    middle = left + ((right - left) >> 1);
    if (array[middle] < target)
      left = middle + 1;
    else
      right = middle;
  }
  return right;
}

} // namespace util

#endif //BIGDATA_RACE_UTILS_H
