#ifndef ACTION_H
#define ACTION_H

#include <cassert>
#include <vector>
#include <stdint.h>
#include "machine.h"
#include <pthread.h>
#include <time.h>
#include <cstring>
#include <city.h>
#include <mv_record.h>
#include <util.h>

#define YCSB_RECORD_SIZE 1000
#define SPIN_DURATION 10000

extern uint32_t NUM_CC_THREADS;
extern uint64_t recordSize;

struct ycsb_record {
        char value[1000];
};

static  __attribute__((unused))
void do_spin()
{
        for (uint64_t i = 0; i < SPIN_DURATION; ++i)
                single_work();
}

#endif // ACTION_H
