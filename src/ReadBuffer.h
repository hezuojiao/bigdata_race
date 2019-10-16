//
// Created by hezuojiao on 2019-10-16.
//

#ifndef BIGDATA_RACE_READBUFFER_H
#define BIGDATA_RACE_READBUFFER_H

#include <cstdio>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "Constants.h"

class ReadBuffer {
 private:
  size_t length;
  size_t position;
  size_t buffer_pos;
  char* buffer;
  int fd;

 public:
  ReadBuffer(char* buf, int _fd) : position(0), buffer_pos(0), buffer(buf), fd(_fd) {
    length = fileSize(fd);
    pread(fd, buffer, BUFFER_SIZE, position);
  }

  ~ReadBuffer() {
    close(this->fd);
  }

  bool hasNext() {
    return position < length;
  }

  char next() {
    if (buffer_pos == BUFFER_SIZE) {
      pread(fd, buffer, BUFFER_SIZE, position);
      buffer_pos = 0;
    }
    ++position;
    return buffer[buffer_pos++];
  }

  static size_t fileSize(int fd_) {
    struct stat stat_buf;
    int rc = fstat(fd_, &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
  }

};

#endif //BIGDATA_RACE_READBUFFER_H
