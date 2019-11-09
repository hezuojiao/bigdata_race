//
// Created by hezuojiao on 2019-11-09.
//

#include <iostream>
#include <sys/mman.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/time.h>
#include <cstring>
#include <cstdlib>
#include <cstdint>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

float get_elapse_time(struct timeval *begin, struct timeval *end){
  return (float(end->tv_sec-begin->tv_sec))*1000 + (float(end->tv_usec-begin->tv_usec))/1000;
}

void readfile(char* fileName) {
  int fd = open(fileName, O_RDONLY , 0777);
  struct stat st;
  stat(fileName, &st);
  size_t len = st.st_size, pos = 0;

  char* base = (char*)mmap(NULL, len, PROT_READ, MAP_SHARED, fd, 0);
  posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);

  char c;
  while (pos < len) {
    c = base[pos++];
  }

  munmap(base, len);
  close(fd);
}

int main(int argc, char* argv[]) {

  struct timeval begin, end;
  gettimeofday(&begin, NULL);
  readfile(argv[1]);
  gettimeofday(&end, NULL);
  std::cout<<"read time used:"<<int(get_elapse_time(&begin, &end))<<" ms"<<std::endl;
}