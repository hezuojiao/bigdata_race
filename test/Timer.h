//
// Created by jing on 2019-10-21.
//

#ifndef BIGDATA_RACE_TIMER_H
#define BIGDATA_RACE_TIMER_H
#include <thread>

using namespace std;


class Timer{
public:
    using float_seconds = std::chrono::duration<float>;
    using time_point = chrono::time_point<std::chrono::system_clock>;
    Timer(){
        start();
    }
    void start(){
        start_ = std::chrono::system_clock::now();
    };

    void reset(){
        start();
    }
    void end(){
        end_ = std::chrono::system_clock::now();
    }
    float duration(){
        end();
        auto dur = end_ - start_;
        auto secs = std::chrono::duration_cast<float_seconds>(dur);
        return secs.count();
    }
private:

    time_point start_;
    time_point end_;
};


#endif //BIGDATA_RACE_TIMER_H
