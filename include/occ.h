#ifndef 	OCC_H_
#define		OCC_H_

#include <runnable.hh>
#include <table.h>
#include <concurrent_queue.h>
#include <occ_action.h>
#include <exception>
#include <record_buffer.h>

struct OCCActionBatch {
        uint32_t batchSize;
        OCCAction **batch;
};

struct occ_log_header {
        uint32_t table_id;
        uint64_t key;
        uint64_t tid;
        uint32_t record_len;
};

struct OCCWorkerConfig {
        SimpleQueue<OCCActionBatch> *inputQueue;
        SimpleQueue<OCCActionBatch> *outputQueue;
        int cpu;
        Table **tables;
        Table **lock_tables;
        bool is_leader;
        volatile uint32_t *epoch_ptr;
        volatile uint64_t num_completed;
        uint64_t epoch_threshold;
        uint64_t log_size;
        bool globalTimestamps;
        uint32_t num_tables;
};


class OCCWorker : public Runnable {
 private:        
        OCCWorkerConfig config;
        uint64_t incr_timestamp;
        uint64_t last_tid;
        uint32_t last_epoch;
        uint32_t txn_counter;
        RecordBuffers *bufs;
        
        virtual bool RunSingle(OCCAction *action);
        virtual uint32_t exec_pending(OCCAction **action_list);
        virtual void UpdateEpoch();
        virtual void EpochManager();
        virtual void TxnRunner();
        
 protected:
        virtual void StartWorking();
        virtual void Init();
 public:
        void* operator new(std::size_t sz, int cpu)
        {
                return alloc_mem(sz, cpu);
        }
        
        OCCWorker(OCCWorkerConfig conf, RecordBuffersConfig rb_conf);
        virtual uint64_t NumCompleted();
};

#endif		// OCC_H_
