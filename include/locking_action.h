#ifndef LOCKING_ACTION_H_
#define LOCKING_ACTION_H_

#include <db.h>
#include <vector>

class locking_action;

struct locking_key {

public:
        locking_key(uint64_t key, uint32_t table_id, bool is_write);
        
        uint32_t table_id;
        uint64_t key;
        bool is_write;
        locking_action *dependency;
        bool is_held;
        volatile uint64_t *latch;
        struct locking_key *prev;
        struct locking_key *next;
        bool is_initialized;
        void *value;

        bool operator==(const struct locking_key &other)
        {
                return this->table_id == other.table_id &&
                this->key == other.key;
        }

        bool operator>(const struct locking_key &other)
        {
                return (this->table_id > other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key > other.key)
                         );
        }

        bool operator<(const struct locking_key &other)
        {
                return (this->table_id < other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key < other.key)
                         );
        }

        bool operator>=(const struct locking_key &other)
        {
                return !(*this < other);
        }

        bool operator<=(const struct locking_key &other)
        {
                return !(*this > other);
        }

        bool operator!=(const struct locking_key &other)
        {
                return !(*this == other);
        }

        static uint64_t hash(const struct locking_key *k)
        {
                Hash128to64(std::make_pair(k->key, (uint64_t)(k->table_id)));
        }
};

class locking_action : public translator {
 private:
        Table **tables;
        bool prepared;
        uint32_t read_index;
        uint32_t write_index;        
        std::vector<locking_key> writeset;
        std::vector<locking_key> readset;        
        
 public:
        locking_action(txn *txn);
        void add_read_key(uint64_t key, uint32_t table_id);
        void add_write_key(uint64_t key, uint32_t table_id);

        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
};

#endif // LOCKING_ACTION_H_
