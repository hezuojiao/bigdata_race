//
// Created by jing on 2019-10-21.
//

#ifndef BIGDATA_RACE_UTILS_H
#define BIGDATA_RACE_UTILS_H
#include <iostream>
#include <thread>
#include <vector>
#include <string>

using namespace std;

template <class T1, class T2, class T3, class T4=string>
void Log(T1 a, T2 b, T3 c="", T4 d="") {
    cout<<"LOG :: "<<a<<": "<<b<<" "<<c<<" "<<d<<"\n";
}


#endif //BIGDATA_RACE_UTILS_H
