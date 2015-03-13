#ifndef HEK_TABLE_H_
#define HEK_TABLE_H_

#include <stdint.h>

struct hek_record {
        struct hek_record *next;
        uint64_t begin;
        uint64_t end;
        uint64_t key;
        uint32_t size;
        char value[0];
};

struct hek_table_slot {
        volatile uint64_t latch;
        volatile hek_record *records;
} __attribute__((__aligned__(64)));

class hek_table {
 private:
        uint64_t num_slots;
        struct hek_table_slot *slots;
        
        struct hek_table_slot* get_slot(uint64_t key);
        bool get_preparing_ts(hek_record *record, uint64_t *ret);
        hek_record* search_stable(uint64_t key, uint64_t ts,
                                  hek_record *iter);
        bool validate(hek_record *cur, hek_record *prev);
        hek_record* search_bucket(uint64_t key, uint64_t ts,
                                  struct hek_table_slot *slot);

 public:
        hek_table(uint64_t num_slots, int cpu_start, int cpu_end);
        hek_record* get_version(uint64_t key, uint64_t ts);
        bool insert_version(hek_record *record);
        void remove_version(hek_record *record, uint64_t ts);
        void finalize_version(hek_record *record, uint64_t ts);
};

#endif // HEK_TABLE_H_
