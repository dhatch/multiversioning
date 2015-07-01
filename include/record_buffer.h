#ifndef RECORD_BUFFER_H_
#define RECORD_BUFFER_H_

#include <stdint.h>
#include <cpuinfo.h>
#include <new>

struct RecordBuffersConfig {
        uint32_t num_tables;
        uint32_t *record_sizes;
        uint32_t num_buffers;
        int cpu;
};

struct RecordBuffy {
        struct RecordBuffy *next;
        char value[0];
};

class RecordBuffers {
 private:
        RecordBuffy **record_lists;
        RecordBuffy **tails;
        static void* AllocBufs(struct RecordBuffersConfig conf);
        static void LinkBufs(struct RecordBuffy *start,
                             uint32_t buf_size,
                             uint32_t num_bufs);
        uint32_t num_records;
 public:
        void* operator new(std::size_t sz, int cpu)
        {
                return alloc_mem(sz, cpu);
        }

        RecordBuffers(struct RecordBuffersConfig conf);        
        void* GetRecord(uint32_t tableId);
        void ReturnRecord(uint32_t tableId, void *record);
        uint32_t NumRecords() { return this->num_records; };
};


#endif // RECORD_BUFFER_H_
