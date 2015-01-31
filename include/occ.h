#ifndef 	OCC_H_
#define		OCC_H_

#include <runnable.hh>
#include <table.h>
#include <concurrent_queue.h>

#define RECORD_TID_PTR(rec_ptr) ((volatile uint64_t*)rec_ptr)
#define RECORD_VALUE_PTR(rec_ptr) ((void*)&(((uint64_t*)rec_ptr)[1]))

struct OCCConfig {
        SimpleQueue<OCCActionBatch> *inputQueue;
        SimpleQueue<OCCActionBatch> *outputQueue;
        int cpu;
        Table **tables;
        bool is_leader;
        volatile uint32_t *epoch;
};

class RecordBuffers {
 public:
        void* GetRecord(uint32_t tableId);
        void ReturnRecord(uint32_t tableId, void *record);
};

class OCCWorker : public Runnable {
 private:
        OCCConfig config;
        uint32_t txn_counter;
        RecordBuffers *bufs;
        virtual void RunSingle(OCCAction *action);
        virtual bool Validate(OCCAction *action);
        virtual void Prepare(OCCAction *action);

 protected:
        virtual void StartWorking();
        virtual void Init();
};

#endif		// OCC_H_
