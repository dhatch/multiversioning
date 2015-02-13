#include <common.h>

TableConfig create_table_config(uint64_t table_id, uint64_t num_buckets,
                                int start_cpu, int end_cpu,
                                uint64_t free_list_sz, uint64_t value_sz)
{
        TableConfig config;
        config.tableId = table_id;
        config.numBuckets = num_buckets;
        config.startCpu = start_cpu;
        config.endCpu = end_cpu;
        config.freeListSz = free_list_sz;
        config.valueSz = value_sz;
        return config;
}

uint64_t GenUniqueKey(RecordGenerator *gen,
                      std::set<uint64_t> *seen_keys)
{
        while (true) {
                uint64_t key = gen->GenNext();
                if (seen_keys->find(key) == seen_keys->end()) {
                        seen_keys->insert(key);
                        return key;
                }
        }
}

void GenRandomSmallBank(char *rec, int len)
{        
        int *temp = (int*)rec;
        len = len/4;
        for (int i = 0; i < len; ++i) {
                temp[i] = rand();
        }
}

timespec diff_time(timespec end, timespec start)
{
        timespec temp;
        if ((end.tv_nsec - start.tv_nsec) < 0) {
                temp.tv_sec = end.tv_sec - start.tv_sec - 1;
                temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
        } else {
                temp.tv_sec = end.tv_sec-start.tv_sec;
                temp.tv_nsec = end.tv_nsec-start.tv_nsec;
        }
        return temp;
}

void gen_random_array(void *array, size_t sz)
{
        size_t len, remainder, i;
        uint32_t *int_array, temp;
        char *byte_array;
        assert(array != NULL);
        len = sz / sizeof(uint32_t);
        remainder = sz % sizeof(uint32_t);
        int_array = (uint32_t*)array;
        byte_array = (char*)&int_array[len];
        for (i = 0; i < len; ++i) 
                int_array[i] = (uint32_t)rand();
        if (remainder > 0) {
                temp = (uint32_t)rand();
                memcpy(byte_array, &temp, remainder);
        }
}
