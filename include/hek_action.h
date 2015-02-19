#ifndef HEK_ACTION_H_
#define HEK_ACTION_H_

#include <action.h>
#include <vector>

class hek_action;

struct hek_key {
        uint64_t key;
        uint32_t table_id;
        hek_action *txn;
};

// Align to 256 bytes because we use the least significant byte
// corresponding to the pointer.
class hek_action {
 public:
        std::vector<hek_key> readset;
        std::vector<hek_key> writeset;
        volatile uint64_t dep_flag;
        volatile uint64_t dep_count;
        uint64_t begin;
        uint64_t end;

        virtual bool Run() = 0;
        
} __attribute__((__aligned__(256)));

#endif // HEK_ACTION_H_
