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


namespace util {

static inline bool file_exists(const char *file_name) {
  struct stat buffer;
  return (stat(file_name, &buffer) == 0);
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

static inline uint32_t partition(uint32_t key) {
  return ((key&0x20)>>2) | (key&0x7);
}

struct PMutex {
  pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;

  void lock() { pthread_mutex_lock(&m); }

  void unlock() { pthread_mutex_unlock(&m); }
};

} // namespace util

#endif //BIGDATA_RACE_UTILS_H
