#ifndef         PREPROCESSOR_H_
#define         PREPROCESSOR_H_

#include <runnable.hh>
#include <concurrent_queue.h>
#include <numa.h>
#include <mv_table.h>

class CompositeKey;
class Action;
class MVRecordAllocator;
class MVTablePartition;

struct ActionBatch {
    Action **actionBuf;
    uint32_t numActions;
};

/*
 * An MVActionHasher is the first stage of the transaction processing pipleline.
 * Its job is to take a batch of transactions as input, and assign each key of 
 * each transaction to a concurrency control worker thread. We hash keys in this
 * stage because it reduces the amount of serial work that must be perfomed by 
 * the concurrency control stage.
 */
class MVActionHasher : public Runnable {
 private:
  
  uint32_t numHashers;

  // A batch of transactions is input through this queue.
  SimpleQueue<ActionBatch> *inputQueue;
  
  // Once a batch of transactions is completed, output to this queue.
  SimpleQueue<ActionBatch> *outputQueue;  

  // 
  SimpleQueue<ActionBatch> **leaderEpochStartQueues;
  SimpleQueue<ActionBatch> **leaderEpochStopQueues;
  
  SimpleQueue<ActionBatch> *subordInputQueue;
  SimpleQueue<ActionBatch> *subordOutputQueue;

 protected:
  
  virtual void StartWorking();
  
  virtual void Init();
  
  static inline void ProcessAction(Action *action, uint32_t epoch, 
                                   uint32_t txnCounter);

 public:
  
  // Override the default allocation mechanism. We want all thread local memory
  // allocations to be 
  // 
  void* operator new(std::size_t sz, int cpu);
  
  // Constructor
  //
  // param numThreads: Number of concurrency control threads (in the next stage)
  // param inputQueue: Queue through which batches are input
  // param outputQueue: Queue through which batches are output
  MVActionHasher(int cpuNumber,
                 SimpleQueue<ActionBatch> *inputQueue, 
                 SimpleQueue<ActionBatch> *outputQueue);
};

struct MVSchedulerConfig {
  int cpuNumber;
  uint32_t threadId;
  size_t allocatorSize;         // Scheduler thread's local sticky allocator
  uint32_t numTables;           // Number of tables in the system
  size_t *tblPartitionSizes;    // Size of each table's partition
    
  // Coordination queues required by the leader thread.
  SimpleQueue<ActionBatch> *leaderInputQueue;
  SimpleQueue<ActionBatch> *leaderOutputQueue;
  SimpleQueue<ActionBatch> **leaderEpochStartQueues;
  SimpleQueue<ActionBatch> **leaderEpochStopQueues;
    
  // Coordination queues required by the subordinate threads.
  SimpleQueue<ActionBatch> *subordInputQueue;
  SimpleQueue<ActionBatch> *subordOutputQueue;
};

/*
 * MVScheduler implements scheduling logic. The scheduler is partitioned across 
 * several physical cores.
 */
class MVScheduler : public Runnable {
        friend class SchedulerTest;
        
 private:
        static inline uint32_t GetCCThread(CompositeKey key);

    MVSchedulerConfig config;
    //  VersionBufferAllocator *alloc;

    MVTablePartition **partitions;

        uint32_t epoch;
        uint32_t txnCounter;
    uint64_t txnMask;

    uint32_t threadId;

 protected:
        virtual void StartWorking();
        void ProcessWriteset(Action *action, uint64_t timestamp);
        void ScheduleTransaction(Action *action, uint64_t version);     
    void Leader(uint32_t epoch);
    void Subordinate(uint32_t epoch);
    virtual void Init();
 public:
        static uint32_t NUM_CC_THREADS;
        MVScheduler(MVSchedulerConfig config);
};


#endif          /* PREPROCESSOR_H_ */
