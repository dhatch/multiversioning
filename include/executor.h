#ifndef         EXECUTOR_H_
#define         EXECUTOR_H_

#include <preprocessor.h>
#include <mv_record.h>
#include <database.h>

struct ActionListNode {
  Action *action;
  ActionListNode *next;
  ActionListNode *prev;
};

struct GarbageBinConfig {
  uint32_t numCCThreads;
  uint32_t numWorkers;
  uint32_t numTables;
  volatile uint32_t *lowWaterMarkPtr;
  
  SimpleQueue<MVRecordList> **ccChannels;
  SimpleQueue<RecordList> ***workerChannels;
};

class GarbageBin {
 private:

  // curStickies and curRecords correspond to "live" queues, in which new 
  // garbage is thrown
  MVRecordList *curStickies;
  RecordList **curRecords;
  
  // snapshotStickies and snapshotRecords correspond to a snapshot as of a 
  // specific epoch
  MVRecordList *snapshotStickies;
  RecordList **snapshotRecords;
  uint32_t snapshotEpoch;

  GarbageBinConfig config;

  void ReturnGarbage();

 public:
  GarbageBin(GarbageBinConfig config);

  void AddRecord(uint32_t workerThread, uint32_t tableId, Record *rec);
  void AddMVRecord(uint32_t ccThread, MVRecord *rec);
  void FinishEpoch(uint32_t epoch);
};

// List of actions still to be completed as part of a particular epoch
class PendingActionList {
 private:
  
  // Allocator
  ActionListNode *freeList;
  
  // List meta-data
  uint32_t pendingCount;
  ActionListNode *head;
  ActionListNode *tail;

  // Used to iterate through list
  ActionListNode *cursor;

 public:
  PendingActionList(uint32_t freeListSize);

  void EnqueuePending(Action *action);
  void DequeuePending(ActionListNode *listNode);

  void ResetCursor();
  ActionListNode* GetNext();
  bool IsEmpty();
};

class RecordAllocator {
 private:
  Record *freeList;
  
 public:
  RecordAllocator(size_t recordSize, uint32_t numRecords, int cpu);  
  bool GetRecord(Record **OUT_REC);
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
  uint32_t numQueuesPerTable;
  SimpleQueue<RecordList> ***recycleQueues;
};

class Executor : public Runnable {
 private:
  ExecutorConfig config;
  MVRecordList *deadRecords;
  GarbageBin *garbageBin;
  PendingActionList *pendingList;
  uint32_t epoch;

  RecordAllocator **allocators;

 protected:
  virtual void StartWorking();
  virtual void Init();
  void ReturnVersion(MVRecord *record);

  void ProcessBatch(const ActionBatch &batch);
  bool ProcessSingle(Action *action);
  bool ProcessTxn(Action *action);



 public:
  Executor(ExecutorConfig config);  
};

#endif          // EXECUTOR_H_
