#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include "Timer.h"
#include "utils.h"
#include "../src/FileUtil.h"
#include <fstream>
#include <random>

#include <sys/shm.h>

using namespace std;

#define filename "shmid"
#define ipcid 1010102

int readid(){
    int shmid;
    fstream  fd;
    fd.open(filename, ios_base::in);
    fd>>shmid;
    fd.close();
    return shmid;
}

void writeid(int id) {
    fstream  fd;
    fd.open(filename, ios_base::out);
    fd<<id;
    fd.close();
}

using T = float ;
int run() {
    constexpr int SIZE = 1'000'000;

    Timer t;
    int shmid;
    T* shmaddr = nullptr;
    /*
    1、查看共享内存，使用命令：ipcs -m
    2、删除共享内存，使用命令：ipcrm -m [shmid]
     */

    // there is one id for one shared memory
    // we write it in the file shmid
    if(!file_exists(filename)) {
        shmid = shmget(ipcid, SIZE*sizeof(T), IPC_CREAT|0666);
        if(shmid == -1) {
            Log("ERROE alloc shm", shmid, ""); // TODO add error info
            exit(EXIT_FAILURE);
        }
        writeid(shmid);

//        ftok()
        shmaddr = (T*)shmat(shmid, 0, 0);// addr
        if(shmaddr == (void*)-1)
        {
            perror("shmget");
            Log("ERROE shmat shm", shmid, errno); // TODO add error info
            exit(EXIT_FAILURE);
        }

        std::mt19937 randmt(1);
        uniform_real_distribution<float > dis2(0.0, 1.0);
        vector<T> va(SIZE);
        fill(va.begin(), va.end(), dis2(randmt));
        Log("inti", "1,9",va[1]+va[9]);
        Log("inti", "2,9",va[2]+va[9]);
        memcpy(shmaddr, va.data(), SIZE*sizeof(T));
    }else{
        shmid = readid();
        shmaddr = (T*)shmat(shmid, 0, 0);// addr
        if(shmaddr == (void*)-1)
        {
            perror("shmget");
            Log("ERROE shmat shm", shmid, errno); // TODO add error info
            exit(EXIT_FAILURE);
        }
    }

    Log("inti", "1,9",shmaddr[1]+shmaddr[9]);
    Log("inti", "2,9",shmaddr[2]+shmaddr[9]);
    // detach
    shmdt(shmaddr);

    Log("ret", t.duration(), "");

    return 0;
}

int main() {
    cout<<file_exists(filename)<<","<<remove(filename)<<endl;
    for(int i=0;i<10;i++) {
        cout<<"i:"<<i<<endl;
        run();
    }
}
