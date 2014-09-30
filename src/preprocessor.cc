#include <preprocessor.h>
#include <machine.h>
#include <city.h>
#include <catalog.h>
#include <database.h>
#include <action.h>
#include <cpuinfo.h>

#include <stdlib.h>

#include <cassert>
#include <cstring>
#include <deque>


using namespace std;

uint32_t MVScheduler::NUM_CC_THREADS = 1;
/*
struct ThreadArgs {
  void *arg;
  volatile uint64_t 
};


void ThreadInit(void *threadArgs, void*) {
  
}

void MVScheduler::Init(MVSchedulerConfig config) {
  
}
*/

void MVScheduler::Init() {
  std::cout << "Called init on core: " << m_cpu_number << "\n";
  auto alloc = new (m_cpu_number) MVRecordAllocator(config.allocatorSize, m_cpu_number);
  this->partition = new MVTablePartition(config.partitionSize, m_cpu_number, alloc);
  this->threadId = config.threadId;
}

MVScheduler::MVScheduler(MVSchedulerConfig config) : 
  Runnable(config.cpuNumber) {

  this->config = config;
  this->epoch = 0;
  this->txnCounter = 0;
  this->txnMask = ((uint64_t)1<<config.threadId);
  
  std::cout << "Thread id: " << config.threadId << "\n";
  std::cout << "Mask: " << txnMask << "\n";

  //    this->alloc = NULL;
}


void MVScheduler::StartWorking() {

    uint32_t epoch = 0;
  while (true) {

    if (config.threadId == 0) {
        Leader(epoch);
    }
    else {
        Subordinate(epoch);
    }
    epoch += 1;

  }
}

static inline uint64_t compute_version(uint32_t epoch, uint32_t txnCounter) {
    return (((uint64_t)epoch << 32) | txnCounter);
}

void MVScheduler::Subordinate(uint32_t epoch) {
  assert(config.threadId != 0 && config.threadId < NUM_CC_THREADS);

  // Take a batch of tranasctions from the leader.
  ActionBatch curBatch = config.subordInputQueue->DequeueBlocking();
    
  // Maintain dependencies for the current batch.
  uint32_t txnCounter = 0;
  for (uint32_t i = 0; i < curBatch.numActions; ++i) {      
      uint64_t version = compute_version(epoch, txnCounter);
      ScheduleTransaction(curBatch.actionBuf[i], version);
      txnCounter += 1;
  }

  // Signal that we're done with the current batch.
  config.subordOutputQueue->EnqueueBlocking(curBatch);
}

void MVScheduler::Leader(uint32_t epoch) {
  assert(config.threadId == 0);

  // Take a batch of transactions from the input queue.
  ActionBatch curBatch = config.leaderInputQueue->DequeueBlocking();

  // Signal every other concurrency control thread to start.
  for (uint32_t i = 0; i < NUM_CC_THREADS-1; ++i) {
    config.leaderEpochStartQueues[i]->EnqueueBlocking(curBatch);
  }
    
  // Maintain dependencies for the current batch of transactions
  uint32_t txnCounter = 0;
  for (uint32_t i = 0; i < curBatch.numActions; ++i) {
    uint64_t version = compute_version(epoch, txnCounter);
    ScheduleTransaction(curBatch.actionBuf[i], version);
    txnCounter += 1;
  }

  // Wait for all concurrency control threads to finish.    
  for (uint32_t i = 0; i < NUM_CC_THREADS-1; ++i) {
    config.leaderEpochStopQueues[i]->DequeueBlocking();
  }
    
  config.leaderOutputQueue->EnqueueBlocking(curBatch);
}

/*
 * Hash the given key, and find which concurrency control thread is
 * responsible for the appropriate key range. 
 */
uint32_t MVScheduler::GetCCThread(CompositeKey key) {
  uint64_t hash = CompositeKey::Hash(&key);
  return (uint32_t)(hash % NUM_CC_THREADS);
}


/*
 * For each record in the writeset, write out a placeholder indicating that
 * the value for the record will be produced by this transaction. We don't need
 * to track the version of each record written by the transaction. The version
 * is equal to the transaction's timestamp.
 */
void MVScheduler::ProcessWriteset(Action *action, uint64_t timestamp) {
    size_t size = action->writeset.size();
    for (uint32_t i = 0; i < size; ++i) {
        if (action->writeset[i].threadId == threadId) {
            this->partition->WriteNewVersion(action->writeset[i], action, timestamp);
        }
    }
}


inline void MVScheduler::ScheduleTransaction(Action *action, uint64_t version) {
  if ((action->combinedHash & txnMask) != 0) {
    ProcessWriteset(action, version);
  }
}
