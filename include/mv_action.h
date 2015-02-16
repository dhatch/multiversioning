#ifndef MV_ACTION_H_
#define MV_ACTION_H_

#include <action.h>

#define RECYCLE_QUEUE_SIZE 64 
#define MV_EPOCH_MASK 0xFFFFFFFF00000000
#define GET_MV_EPOCH(timestamp) (timestamp & MV_EPOCH_MASK)
#define CREATE_MV_TIMESTAMP(epoch, timestamp) ((((uint64_t)epoch)<<32) | timestamp)

struct ActionBatch {
    Action **actionBuf;
    uint32_t numActions;
};

enum ActionState {
        STICKY,
        PROCESSING,
        SUBSTANTIATED,
};

struct Record {
        Record *next;
        char value[0];
};

struct RecordList {
        Record *head;
        Record **tail;
        uint64_t count;
};

class CompositeKey {
 public:
        uint32_t tableId;
        uint64_t key;
        uint32_t threadId;
        bool is_rmw;
        MVRecord *value;

        CompositeKey() {
                this->value = 0;
        }
        
        CompositeKey(bool isRmw, uint32_t table, uint64_t key) {
                this->is_rmw = isRmw;
                this->tableId = table;
                this->key = key;
                this->value = NULL;
        }
  
        CompositeKey(bool isRmw) {
                this->is_rmw = isRmw;
                this->tableId = 0;
                this->key = 0;
                this->value = NULL;
        }

        bool operator==(const CompositeKey &other) const {
                return other.tableId == this->tableId && other.key == this->key;
        }

        bool operator!=(const CompositeKey &other) const {
                return !(*this == other);
        }
  
        bool operator<(const CompositeKey &other) const {
                return ((this->tableId < other.tableId) || 
                        ((this->tableId == other.tableId) && (this->key < other.key)));
        }
  
        bool operator>(const CompositeKey &other) const {
                return ((this->tableId > other.tableId) ||
                        ((this->tableId == other.tableId) && (this->key > other.key)));
        }
  
        bool operator<=(const CompositeKey &other) const {
                return !(*this > other);
        }
  
        bool operator>=(const CompositeKey &other) const {
                return !(*this < other);
        }

        static inline uint64_t Hash(const CompositeKey *key) {
                return Hash128to64(std::make_pair(key->key, (uint64_t)(key->tableId)));
        }
  
        static inline uint64_t HashKey(const CompositeKey *key) {
                return Hash128to64(std::make_pair((uint64_t)key->tableId, key->key));
        }

} __attribute__((__packed__, __aligned__(64)));

class Action {
 protected:
        CompositeKey GenerateKey(bool is_rmw, uint32_t tableId, uint64_t key);
        void* Read(uint32_t index);
        void* GetWriteRef(uint32_t index);
        void* ReadWrite(uint32_t index);
  
 public:  
        uint64_t __version;
        uint64_t __combinedHash;
        bool __readonly;
        std::vector<CompositeKey> __readset;
        std::vector<CompositeKey> __writeset;
        volatile uint64_t __attribute__((aligned(CACHE_LINE))) __state;

        Action();
        
        virtual bool Run() = 0;
        virtual void AddReadKey(uint32_t tableId, uint64_t key);
        virtual void AddWriteKey(uint32_t tableId, uint64_t key, bool is_rmw);
};

class InsertAction : public Action {
 public:
        virtual bool Run();
};

class mv_readonly : public Action {
 protected:
        char __reads[1000];
 public:
        mv_readonly();
        bool Run();
};

class mv_mix_action : public mv_readonly {
 public:
        bool Run();
};

class RMWAction : public Action {
        volatile uint64_t __total;
        uint64_t __accumulated[1000/sizeof(uint64_t)];

        void DoReads();
        void AccumulateValues();
        void DoWrites();
        
 public:
        RMWAction(uint64_t seed);
        virtual bool Run();
};


#endif // MV_ACTION_H_

