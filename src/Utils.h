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

static bool file_exists(const char *file_name) {
  struct stat buffer;
  return (stat(file_name, &buffer) == 0);
}

static size_t file_size(const char *file_name) {
  struct stat st;
  stat(file_name, &st);
  size_t size = st.st_size;
  return size;
}

static int DateToInt(const char* date) {
  int i_date = 0;
  for (int i = 0; date[i] != '\0'; i++) {
    i_date = date[i] != '-' ? i_date * 10 + date[i] - '0' : i_date;
  }
  return i_date;
}

} // namespace util

#endif //BIGDATA_RACE_UTILS_H
