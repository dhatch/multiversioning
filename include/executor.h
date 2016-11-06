#ifndef         EXECUTOR_H_
#define         EXECUTOR_H_

#include <scheduler.h>
#include <mv_record.h>
#include <database.h>
#include <set>

struct ActionListNode {
  mv_action *action;
  ActionListNode *next;
  ActionListNode *prev;
};

/*
struct CCGarbageChannels {
  uint32_t numChannels;
  SimpleQueue<MVRecordList> *ccChannels[0];
};

struct WorkerGarbageChannels {
  uint32_t numTables;
  uint32_t numWorkers;
  SimpleQueue<RecordList> *workerChannels[0];
};
*/

struct GarbageBinConfig {
  uint32_t numCCThreads;
  uint32_t numWorkers;
  uint32_t numTables;
  int cpu;
  volatile uint32_t *lowWaterMarkPtr;
  
  SimpleQueue<MVRecordList> **ccChannels;
  SimpleQueue<RecordList> **workerChannels;
};

class GarbageBin {
 private:

  // curStickies and curRecords correspond to "live" queues, in which new 
  // garbage is thrown
  MVRecordList *curStickies;
  RecordList *curRecords;
  
  // snapshotStickies and snapshotRecords correspond to a snapshot as of a 
  // specific epoch
  MVRecordList *snapshotStickies;
  RecordList *snapshotRecords;
  uint32_t snapshotEpoch;

  GarbageBinConfig config;

  void ReturnGarbage();

 public:
  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }
  
  GarbageBin(GarbageBinConfig config);

  void AddRecord(uint32_t workerThread, uint32_t tableId, Record *rec);
  void AddMVRecord(uint32_t ccThread, MVRecord *rec);
  void FinishEpoch(uint32_t epoch);
};

/* List of actions still to be completed as part of a particular epoch. */
class PendingActionList {
 private:  
        /* Allocator */
        ActionListNode *freeList;
  
        /* List meta-data */
        ActionListNode *head;
        ActionListNode *tail;

        /* Used to iterate through list */
        ActionListNode *cursor;
  
        uint32_t size;

 public:
        void* operator new(std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }

        PendingActionList(uint32_t freeListSize);

        void EnqueuePending(mv_action *action);
        void DequeuePending(ActionListNode *listNode);

        void ResetCursor();
        ActionListNode* GetNext();
        bool IsEmpty();
        uint32_t Size();
};

class RecordAllocator {
 private:
  Record *freeList;
  
 public:
  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }

  RecordAllocator(size_t recordSize, uint32_t numRecords, int cpu);  
  bool GetRecord(Record **OUT_REC);
  void FreeSingle(Record *rec);
  void Recycle(RecordList recList);
};

struct ExecutorConfig {
        uint32_t threadId;
        uint32_t numExecutors;
        int cpu;
        volatile uint32_t *epochPtr;
        volatile uint32_t *lowWaterMarkPtr;
        SimpleQueue<ActionBatch> *inputQueue;
        SimpleQueue<ActionBatch> *outputQueue;
        uint32_t numTables;
        uint64_t *recordSizes;
        uint64_t *allocatorSizes;
        uint32_t numQueuesPerTable;
        SimpleQueue<RecordList> *recycleQueues;
        GarbageBinConfig garbageConfig;
};

class Executor : public Runnable {
 private:
        ExecutorConfig config;
        GarbageBin *garbageBin;
        PendingActionList *pendingList;
        uint32_t epoch;

        RecordAllocator **allocators;
        void **bufs;
        uint64_t buf_ptr;
        PendingActionList *pendingGC;
        uint64_t counter;

 protected:

        //  Executor(ExecutorConfig config);
        virtual void StartWorking();
        virtual void Init();
        void ReturnVersion(MVRecord *record);

        void ExecPending();

        void ProcessBatch(const ActionBatch &batch);
        bool ProcessSingle(mv_action *action);
        bool ProcessTxn(mv_action *action);

        bool run_readonly(mv_action *action);
        void RecycleData();

        void adjust_lowwatermark();

        uint32_t DoPendingGC();
        bool ProcessSingleGC(mv_action *action);
        bool check_ready(mv_action *action);

 public:
        void* operator new(std::size_t sz, int cpu) {
                return alloc_mem(sz, cpu);
        }

        Executor(ExecutorConfig config);
};

#endif          // EXECUTOR_H_

