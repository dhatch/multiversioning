#ifndef 	OCC_H_
#define		OCC_H_

#include <runnable.hh>
#include <table.h>
#include <concurrent_queue.h>

struct OCCConfig {
        SimpleQueue<OCCActionBatch> *inputQueue;
        SimpleQueue<OCCActionBatch> *outputQueue;
        int cpu;
        Table **tables;
};

class RecordBuffers {
 public:
        void* GetRecord(uint32_t tableId);
        void ReturnRecord(uint32_t tableId, void *record);
};

class OCCWorker : public Runnable {
 private:
        OCCConfig config;
        RecordBuffers *bufs;
        virtual void RunSingle(OCCAction *action);
        virtual bool Validate(OCCAction *action);
        virtual void Prepare(OCCAction *action);

 protected:
        virtual void StartWorking();
        virtual void Init();
};

#endif		// OCC_H_
