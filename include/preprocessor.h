#ifndef 	PREPROCESSOR_H_
#define 	PREPROCESSOR_H_

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

struct MVSchedulerConfig {
  int cpuNumber;
  uint32_t threadId;
  size_t allocatorSize;
  size_t partitionSize;

    
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
    //	VersionBufferAllocator *alloc;

    MVTablePartition *partition;

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


#endif 		/* PREPROCESSOR_H_ */
