#ifndef LOCKING_ACTION_H_
#define LOCKING_ACTION_H_

#include <db.h>
#include <table.h>
#include <vector>
#include <machine.h>
#include <record_buffer.h>

class locking_action;
class lock_manager_table;
class locking_worker;

struct locking_key {

public:
        locking_key(uint64_t key, uint32_t table_id, bool is_write);
        locking_key();
        
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

        bool operator==(const struct locking_key &other) const
        {
                return this->table_id == other.table_id &&
                this->key == other.key;
        }

        bool operator>(const struct locking_key &other) const
        {
                return (this->table_id > other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key > other.key)
                         );
        }

        bool operator<(const struct locking_key &other) const
        {
                return (this->table_id < other.table_id) ||
                        (
                         (this->table_id == other.table_id) &&
                         (this->key < other.key)
                         );
        }

        bool operator>=(const struct locking_key &other) const
        {
                return !(*this < other);
        }

        bool operator<=(const struct locking_key &other) const
        {
                return !(*this > other);
        }

        bool operator!=(const struct locking_key &other) const
        {
                return !(*this == other);
        }

        static uint64_t hash(const struct locking_key *k)
        {
                return Hash128to64(std::make_pair(k->key,
                                                  (uint64_t)(k->table_id)));
        }

        uint64_t Hash() const
        {
                return locking_key::hash(this);
        }
};

class locking_action : public translator {
        friend class LockManagerTable;
        friend class LockManager;
        friend class locking_worker;
        
 private:
        locking_action();
        locking_action(const locking_action&);
        locking_action& operator=(const locking_action&);
        
        volatile uint64_t __attribute__((__aligned__(CACHE_LINE)))
                num_dependencies;
        locking_worker *worker;
        locking_action *next;
        locking_action *prev;
        Table **tables;
        bool prepared;
        uint32_t read_index;
        uint32_t write_index;
        bool finished_execution;
        RecordBuffers *bufs;
        
        std::vector<locking_key> writeset;
        std::vector<locking_key> readset;        

        void commit_writes(bool commit);
        void* lookup(locking_key *key);
        
        int find_key(uint64_t key, uint32_t table_id,
                     std::vector<locking_key> key_list);
        
 public:
        locking_action(txn *txn);
        void add_read_key(uint64_t key, uint32_t table_id);
        void add_write_key(uint64_t key, uint32_t table_id);

        void* write_ref(uint64_t key, uint32_t table_id);
        void* read(uint64_t key, uint32_t table_id);
        int rand();
        void prepare();
        bool Run();
};

#endif // LOCKING_ACTION_H_
