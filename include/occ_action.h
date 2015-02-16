#ifndef OCC_ACTION_H_
#define OCC_ACTION_H_

#include <action.h>

#define TIMESTAMP_MASK (0xFFFFFFFFFFFFFFF0)
#define EPOCH_MASK (0xFFFFFFFF00000000)

#define CREATE_TID(epoch, counter) ((((uint64_t)epoch)<<32) | (((uint64_t)counter)<<4))
#define GET_TIMESTAMP(tid) (tid & TIMESTAMP_MASK)
#define GET_EPOCH(tid) ((tid & EPOCH_MASK)>>32)
#define GET_COUNTER(tid) (GET_TIMESTAMP(tid) & ~EPOCH_MASK)
#define IS_LOCKED(tid) ((tid & ~(TIMESTAMP_MASK)) == 1)

struct occ_txn_status {
        bool validation_pass;
        bool commit;
};


class occ_composite_key {
 public:
        uint32_t tableId;
        uint64_t key;
        volatile uint64_t old_tid;
        bool is_rmw;
        void *value;

        occ_composite_key(uint32_t tableId, uint64_t key, bool is_rmw);
        void* GetValue();
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


class OCCAction {
 public:
        uint64_t __tid;
        std::vector<occ_composite_key> readset;
        std::vector<occ_composite_key> writeset;

        // XXX don't require this field anymore. Manually lookup hash tables for
        // refs to records.
        // std::vector<void*> write_records; 
        
        virtual occ_txn_status Run() = 0;
        void AddReadKey(uint32_t table_id, uint64_t key, bool is_rmw);
        void AddWriteKey(uint32_t table_id, uint64_t key);
}; 

class readonly_action : public OCCAction {
 protected:
        char __reads[1000];
                
 public:
        readonly_action();
        virtual occ_txn_status Run();
};

class mix_occ_action : public readonly_action {
 public:
        mix_occ_action();
        virtual occ_txn_status Run();
};


class RMWOCCAction : public OCCAction {
 private:
        uint64_t __accumulated[1000/sizeof(uint64_t)];
        volatile uint64_t __total;

        bool DoReads();
        void AccumulateValues();
        void DoWrites();
 public:
        virtual occ_txn_status Run();
        virtual void* GetData();
};



#endif // OCC_ACTION_H_
