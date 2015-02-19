#ifndef HEK_RECORD_H_
#define HEK_RECORD_H_

struct hek_record {
        uint64_t begin;
        uint64_t end;
        uint64_t key;
        void *value;
};

#endif  // HEK_RECORD_H_
