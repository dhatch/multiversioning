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

void* MVActionHasher::operator new(std::size_t sz, int cpu) {
  void *ret = alloc_mem(sz, cpu);
  assert(ret != NULL);
  return ret;
}

void MVActionHasher::Init() {
}

MVActionHasher:: MVActionHasher(int cpuNumber,
                                SimpleQueue<ActionBatch> *inputQueue, 
                                SimpleQueue<ActionBatch> *outputQueue) 
  : Runnable(cpuNumber){
  this->inputQueue = inputQueue;
  this->outputQueue = outputQueue;
}


void MVActionHasher::StartWorking() {
  uint32_t epoch = 0;
  while (true) {
    
    // Take a single batch as input.
    ActionBatch batch = inputQueue->DequeueBlocking();
    
    // Process every action in the batch.
    uint32_t numActions = batch.numActions;
    for (uint32_t i = 0; i < numActions; ++i) {
      ProcessAction(batch.actionBuf[i], epoch, i);
    }
    
    // Output the batch to the concurrency control stage.
    outputQueue->EnqueueBlocking(batch);
  }
}

void MVActionHasher::ProcessAction(Action *action, uint32_t epoch,
                                   uint32_t txnCounter) {
  action->combinedHash =  0;
  action->version = (((uint64_t)epoch << 32) | txnCounter);
  size_t numWrites = action->writeset.size();  
  for (uint32_t i = 0; i < numWrites; ++i) {
    
    // Find which concurrency control thread is in charge of this key. Write out
    // the threadId and change the combinedHash bitmask appropriately.
      action->writeset[i].threadId = 0;
    uint32_t threadId = 
      CompositeKey::HashKey(&action->writeset[i]) % 
      MVScheduler::NUM_CC_THREADS;
    action->writeset[i].threadId = threadId;
    action->combinedHash |= (((uint64_t)1)<<threadId);
  }
}

void MVScheduler::Init() {
  std::cout << "Called init on core: " << m_cpu_number << "\n";

  this->partitions = 
    (MVTablePartition**)lock_malloc(sizeof(MVTablePartition*)*config.numTables);
  assert(this->partitions != NULL);

  // Initialize the allocator and the partitions.
  this->alloc = new (m_cpu_number) MVRecordAllocator(config.allocatorSize, 
                                                    m_cpu_number);
  for (uint32_t i = 0; i < this->config.numTables; ++i) {

    // Track the partition locally and add it to the database's catalog.
    this->partitions[i] = new MVTablePartition(config.tblPartitionSizes[i],
                                               m_cpu_number, alloc);
    assert(this->partitions[i] != NULL);
    DB.PutPartition(i, config.threadId, this->partitions[i]);
  }
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

static inline uint64_t compute_version(uint32_t epoch, uint32_t txnCounter) {
    return (((uint64_t)epoch << 32) | txnCounter);
}

void MVScheduler::StartWorking() {
  std::cout << config.numRecycleQueues << "\n";
  uint32_t epoch = 0;
  while (true) {
    
    // Take an input batch
    ActionBatch curBatch;// = config.inputQueue->DequeueBlocking();
    while (!config.inputQueue->Dequeue(&curBatch)) {
      Recycle();
    }

    // Signal subordinates
    for (uint32_t i = 0; i < config.numSubords; ++i) {
      config.pubQueues[i]->EnqueueBlocking(curBatch);
    }

    // Process batch of txns
    uint32_t txnCounter = 0;
    for (uint32_t i = 0; i < curBatch.numActions; ++i) {
      uint64_t version = compute_version(epoch, txnCounter);
      ScheduleTransaction(curBatch.actionBuf[i], version);
      txnCounter += 1;
      if (i % 1000 == 0) {
        Recycle();
      }
    }
    
    // Wait for subordinates
    for (uint32_t i = 0; i < config.numSubords; ++i) {
      config.subQueues[i]->DequeueBlocking();
    }    
    
    // Signal that we're done
    for (uint32_t i = 0; i < config.numOutputs; ++i) {
      config.outputQueues[i].EnqueueBlocking(curBatch);
    }
    
    // Check for recycled MVRecords
    /*
    for (uint32_t i = 0; i < config.numRecycleQueues; ++i) {
      MVRecordList recycled;
      while (config.recycleQueues[i]->Dequeue(&recycled)) {
        std::cout << "Recycled!\n";
        this->alloc->ReturnMVRecords(recycled);
      }
    }
    */
    //    std::cout << "Done epoch";
    epoch += 1;
  }
}

void MVScheduler::Recycle() {
  // Check for recycled MVRecords
  for (uint32_t i = 0; i < config.numRecycleQueues; ++i) {
    MVRecordList recycled;
    while (config.recycleQueues[i]->Dequeue(&recycled)) {
      //      std::cout << "Received recycled mv records: " << recycled.count << "\n";
      this->alloc->ReturnMVRecords(recycled);
    }
  }  
}

/*
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
*/


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
  /*
  while (alloc->Warning()) {
    std::cout << "Warning...\n";
    Recycle();
  }
  */
    size_t size = action->writeset.size();
    for (uint32_t i = 0; i < size; ++i) {
        if (action->writeset[i].threadId == threadId) {
            this->partitions[action->writeset[i].tableId]->
              WriteNewVersion(action->writeset[i], action, action->version);
        }
    }
}


inline void MVScheduler::ScheduleTransaction(Action *action, uint64_t version) {
  if ((action->combinedHash & txnMask) != 0) {
    ProcessWriteset(action, version);
  }
}
