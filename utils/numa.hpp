#pragma once

#include <errno.h>      // errno
#include <sched.h>      // sched_setaffinity, getcpu
#include <stdio.h>      // fflush
#include <stdlib.h>     // exit
#include <string.h>     // strerror
#include <sys/types.h>  // pid_t

#include <cassert>

#define LOGICAL_CORE_SIZE 64

class Numa {
    private:
        pid_t tid_;
        
        void schedSetaffinity(cpu_set_t&);
        void schedGetaffinity(cpu_set_t&);
        void cpuSet(cpu_set_t&, const int);
        void getCpu();
        
    public:
        unsigned int cpu_;
        unsigned int node_;
        Numa(pid_t, const int);
};

void Numa::schedGetaffinity(cpu_set_t& cpu_set){
    if (sched_getaffinity(tid_, sizeof(cpu_set), &cpu_set)) {
        printf("sched_getaffinity: errno = %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void Numa::cpuSet(cpu_set_t& cpu_set, const int cpu) {
    CPU_ZERO(&cpu_set);      // どのcpuも含まれない状態にする 000000
    CPU_SET(cpu, &cpu_set);  // cpuを追加.  000010 (=> core1にアサイン)
}

void Numa::schedSetaffinity(cpu_set_t& cpu_set) {
    if (sched_setaffinity(tid_, sizeof(cpu_set), &cpu_set)) {
        printf("sched_setaffinity: errno = %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

void Numa::getCpu() {
    if (getcpu(&cpu_, &node_)) {
        printf("getcpu: errno = %d, %s\n", errno, strerror(errno));
        exit(EXIT_FAILURE);
    }
}

Numa::Numa(pid_t tid, const int target_cpu) : tid_(tid) {
    assert(target_cpu < LOGICAL_CORE_SIZE);
    cpu_set_t cpu_set;

    cpuSet(cpu_set, target_cpu); // cpu_setを作る （core1に配置したければ、cpu_set: 0000010）
    schedSetaffinity(cpu_set); // cpu_setに書かれてある通りにコアを配置
  
    // コアが正しく配置されたかチェック
    getCpu();
    assert(target_cpu == cpu_);
};