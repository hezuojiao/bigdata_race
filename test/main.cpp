#include <iostream>
#include <thread>
#include <vector>
#include <string>
#include <immintrin.h>
#include "Timer.h"

using namespace std;

template <class T>
void init_data(T &va, T &vb) {
    fill(va.begin(), va.end(),rand());
    fill(vb.begin(), vb.end(),rand());
}


template <class T1, class T2, class T3, class T4=string>
void Log(T1 a, T2 b, T3 c="", T4 d="") {
    cout<<"LOG :: "<<a<<": "<<b<<" "<<c<<" "<<d<<"\n";
}

template <class T>
void test_for(T *a, T *b, T *c, int len) {
    for(int i=0;i<len;i++){
        c[i] = a[i]*b[i];
    }
    return ;
}

template <class T>
void test_avx(T *a, T *b, T *d, int len) {
    int i;
    for (i = 0; i <= len - 8; i += 8)
    {
        __m256 x = _mm256_loadu_ps(a + i);
        __m256 y = _mm256_loadu_ps(b + i);
        __m256 z = _mm256_mul_ps(x, y);
        _mm256_storeu_ps(d + i, z);
    }
    for ( ; i < len; i++)
    {
        d[i] = a[i] * b[i];
    }
}

template <class T>
bool check_data(T &va, T &vb, int len) {
    int cnt = 0;
    for(int i=0; i<len; i++) {
        if(va[i] != vb[i]) {
            cnt++;
        }
    }
    if(cnt != 0) cout<<"diff: "<<cnt;
    return cnt==0;
}

using T = float;
/*
# result
## -O0
LOG :: alloc: 6.71725 s
LOG :: build data: 0.680169 s
LOG :: for loop: 0.366461 s
LOG :: avx : 0.151182 s
checking pass
LOG :: check: 0.407603 s

## -O3
LOG :: alloc: 1.30162 s
LOG :: build data: 0.121267 s
LOG :: for loop: 0.100554 s
LOG :: avx : 0.091902 s
checking pass
LOG :: check: 0.040744 s
 */
int main() {
    constexpr int SIZE = 100'000'000;

    /// alloc
    Timer t;
    vector<T> va(SIZE);
    vector<T> vb(SIZE);
    vector<T> vf(SIZE,0);
    vector<T> vavx(SIZE,0);
    Log("alloc", t.duration(),"s");

    /// init data
    t.reset();
    init_data(va, vb);
    Log("build data", t.duration(), "s");

    /// for
    t.reset();
    test_for(va.data(), vb.data(), vf.data(), SIZE);
    Log("for loop", t.duration(), "s");

    /// avx
    t.reset();
    test_avx(va.data(), vb.data(), vavx.data(), SIZE);
    Log("avx ", t.duration(), "s");

    /// check
    t.reset();
    cout<<"checking "<<(check_data(vf, vavx, SIZE)? "pass": "fail")<<"\n";
    Log("check", t.duration(), "s");

    return 0;

}
