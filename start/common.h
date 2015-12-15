#ifndef COMMON_H_
#define COMMON_H_

#include <set>
#include <concurrent_queue.h>
#include <table.h>
#include <record_generator.h>
#include <db.h>
#include <time.h>

#define RMW_COUNT	8
#define MAX_CPU 	79
#define FAKE_ITER_SIZE 1000

template<class T>
SimpleQueue<T>** setup_queues(int num_queues, int queue_size)
{
        SimpleQueue<T> **ret =
                (SimpleQueue<T>**)malloc(sizeof(SimpleQueue<T>*)*num_queues);
        for (int i = 0; i < num_queues; ++i) {
                char *data = (char*)malloc(CACHE_LINE*queue_size);
                ret[i] = new SimpleQueue<T>(data, queue_size);
        }
        return ret;
}

TableConfig create_table_config(uint64_t table_id, uint64_t num_buckets,
                                int start_cpu, int end_cpu,
                                uint64_t free_list_sz, uint64_t value_sz);

uint64_t GenUniqueKey(RecordGenerator *gen, std::set<uint64_t> *seen_keys);

void GenRandomSmallBank(char *rec, int len);

timespec diff_time(timespec end, timespec start);

void gen_random_array(void *array, size_t sz);

void pin_memory();

Table** setup_hash_tables(uint32_t num_tables, uint32_t *num_records, bool occ);

struct big_key* setup_array(txn *t);

#endif // COMMON_H_
