#ifndef OCC_ACTION_H_
#define OCC_ACTION_H_

#include <action.h>
#include <table.h>
#include <db.h>
#include <record_buffer.h>

#define TIMESTAMP_MASK (0xFFFFFFFFFFFFFFF0)
#define EPOCH_MASK (0xFFFFFFFF00000000)

#define CREATE_TID(epoch, counter) ((((uint64_t)epoch)<<32) | (((uint64_t)counter)<<4))
#define GET_TIMESTAMP(tid) (tid & TIMESTAMP_MASK)
#define GET_EPOCH(tid) ((tid & EPOCH_MASK)>>32)
#define GET_COUNTER(tid) (GET_TIMESTAMP(tid) & ~EPOCH_MASK)
#define IS_LOCKED(tid) ((tid & ~(TIMESTAMP_MASK)) == 1)
#define RECORD_TID_PTR(rec_ptr) ((volatile uint64_t*)rec_ptr)
#define RECORD_VALUE_PTR(rec_ptr) ((void*)&(((uint64_t*)rec_ptr)[1]))
#define OCC_RECORD_SIZE(value_sz) (sizeof(uint64_t)+value_sz)
#define REAL_RECORD_SIZE(value_sz) (value_sz - sizeof(uint64_t))

enum validation_err_t {
        READ_ERR,
        VALIDATION_ERR,
};

class OCCWorker;

class occ_validation_exception : public std::exception {
 public:
        occ_validation_exception(validation_err_t err) { this->err = err; }
        validation_err_t err;
};



struct occ_txn_status {
        bool validation_pass;
        bool commit;
};


class occ_composite_key {
 public:
        uint32_t tableId;
        uint64_t key;
        uint64_t old_tid;
        bool is_rmw;
        bool is_locked;
        bool is_initialized;
        void *value;

        occ_composite_key(uint32_t tableId, uint64_t key, bool is_rmw);
        void* GetValue() const ;
        uint64_t GetTimestamp();
        bool ValidateRead();

        void* StartRead();
        bool FinishRead();
        
        bool operator==(const occ_composite_key &other) const {
                return other.tableId == this->tableId && other.key == this->key;
        }

        bool operator!=(const occ_composite_key &other) const {
                return !(*this == other);
        }
  
        bool operator<(const occ_composite_key &other) const {
                return ((this->tableId < other.tableId) || 
                        ((this->tableId == other.tableId) && (this->key < other.key)));
        }
  
        bool operator>(const occ_composite_key &other) const {
                return ((this->tableId > other.tableId) ||
                        ((this->tableId == other.tableId) && (this->key > other.key)));
        }
  
        bool operator<=(const occ_composite_key &other) const {
                return !(*this > other);
        }
  
        bool operator>=(const occ_composite_key &other) const {
                return !(*this < other);
        }
};


class OCCAction : public translator {
        friend class OCCWorker;

 private:
        OCCAction();
        OCCAction& operator=(const OCCAction&);
        OCCAction(const OCCAction&);

        RecordBuffers *record_alloc;
        Table **tables;
        uint64_t tid;
        OCCWorker *worker;
        std::vector<occ_composite_key> readset;
        std::vector<occ_composite_key> writeset;
        std::vector<occ_composite_key> shadow_writeset;

        virtual uint64_t stable_copy(uint64_t key, uint32_t table_id,
                                     void *record); 
        virtual void validate_single(occ_composite_key &comp_key);
        virtual void cleanup_single(occ_composite_key &comp_key);
        virtual void install_single_write(occ_composite_key &comp_key);
        
 public:
        
        OCCAction(txn *txn);
        OCCAction *link;
        
        virtual void *write_ref(uint64_t key, uint32_t table);
        virtual void *read(uint64_t key, uint32_t table);
        virtual int rand();
        
        virtual void set_allocator(RecordBuffers *buf);
        virtual void set_tables(Table **tables);

        virtual bool run();
        virtual void acquire_locks();
        virtual void validate();
        virtual uint64_t compute_tid(uint32_t epoch, uint64_t last_tid);
        virtual void install_writes();
        virtual void release_locks();
        virtual void cleanup();
        
        void add_read_key(uint32_t table_id, uint64_t key);
        void add_write_key(uint32_t table_id, uint64_t key, bool is_rmw);
}; 

#endif // OCC_ACTION_H_
