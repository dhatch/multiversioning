#ifndef DB_H_
#define DB_H_

#include <unordered_map>

class txn;

/*
 * Interface for all database implementations. We want to keep a uniform 
 * interface so that we have a single benchmark implementation that does not 
 * have to be repeated for each baseline we want to measure.
 */
class translator {
 protected:
        txn *t;
        
 public:
        translator(txn *t) { this->t = t; };
        virtual void *write_ref(uint64_t key, uint32_t table) = 0;
        virtual void *read(uint64_t key, uint32_t table) = 0;
};

/*
 * Every transaction implementation must conform to this interface. Independent 
 * of concurrency control technique used.
 */
class txn {
 private:
        translator *trans;
 protected:
        void* get_write_ref(uint64_t key, uint32_t table_id);
        void* get_read_ref(uint64_t key, uint32_t table_id);
 public:
        virtual bool Run() = 0;
};


#endif // DB_H_
