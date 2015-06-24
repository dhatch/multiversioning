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

void MVActionHasher::ProcessAction(mv_action *action, uint32_t epoch,
                                   uint32_t txnCounter) {
  action->__combinedHash =  0;
  action->__version = (((uint64_t)epoch << 32) | txnCounter);
  size_t numWrites = action->__writeset.size();  
  for (uint32_t i = 0; i < numWrites; ++i) {
    
    // Find which concurrency control thread is in charge of this key. Write out
    // the threadId and change the combinedHash bitmask appropriately.
      action->__writeset[i].threadId = 0;
    uint32_t threadId = 
      CompositeKey::HashKey(&action->__writeset[i]) % 
      MVScheduler::NUM_CC_THREADS;
    action->__writeset[i].threadId = threadId;
    action->__combinedHash |= (((uint64_t)1)<<threadId);
  }
}

void MVScheduler::Init() {

}

MVScheduler::MVScheduler(MVSchedulerConfig config) : 
  Runnable(config.cpuNumber) {

  this->config = config;
  this->epoch = 0;
  this->txnCounter = 0;
  this->txnMask = ((uint64_t)1<<config.threadId);

  //  std::cout << "Thread id: " << config.threadId << "\n";
  //  std::cout << "Mask: " << txnMask << "\n";

  //    this->alloc = NULL;

  this->partitions = 
          (MVTablePartition**)alloc_mem(sizeof(MVTablePartition*)*config.numTables, 
                                        config.cpuNumber);
  assert(this->partitions != NULL);

  // Initialize the allocator and the partitions.
  this->alloc = new (config.cpuNumber) MVRecordAllocator(config.allocatorSize, 
                                                         config.cpuNumber,
                                                         config.worker_start,
                                                         config.worker_end);
  for (uint32_t i = 0; i < this->config.numTables; ++i) {

          // Track the partition locally and add it to the database's catalog.
          this->partitions[i] =
                  new (config.cpuNumber) MVTablePartition(config.tblPartitionSizes[i],
                                                          config.cpuNumber, alloc);
          assert(this->partitions[i] != NULL);
          //    DB.PutPartition(i, config.threadId, this->partitions[i]);
  }
  this->threadId = config.threadId;
}

static inline uint64_t compute_version(uint32_t epoch, uint32_t txnCounter) {
    return (((uint64_t)epoch << 32) | txnCounter);
}

void MVScheduler::StartWorking() {
        //  std::cout << config.numRecycleQueues << "\n";
  while (true) {
    ActionBatch curBatch = config.inputQueue->DequeueBlocking();
    for (uint32_t i = 0; i < config.numSubords; ++i) 
      config.pubQueues[i]->EnqueueBlocking(curBatch);
    for (uint32_t i = 0; i < curBatch.numActions; ++i) 
            ScheduleTransaction(curBatch.actionBuf[i]);
    for (uint32_t i = 0; i < config.numSubords; ++i) 
      config.subQueues[i]->DequeueBlocking();
    for (uint32_t i = 0; i < config.numOutputs; ++i) 
      config.outputQueues[i].EnqueueBlocking(curBatch);
    Recycle();
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
void MVScheduler::ProcessWriteset(mv_action *action)
{

        while (alloc->Warning()) {
                //          std::cerr << "[WARNING] CC thread low on versions\n";
                Recycle();
        }

        int r_index = action->__read_starts[threadId];
        int w_index = action->__write_starts[threadId];
        int i;
        while (r_index != -1) {
                i = r_index;
                MVRecord *ref = this->partitions[action->__readset[i].tableId]->
                        GetMVRecord(action->__readset[i], action->__version);
                action->__readset[i].value = ref;
                r_index = action->__readset[i].next;
                
        }

        while (w_index != -1) {
                i = w_index;
                this->partitions[action->__writeset[i].tableId]->
                        WriteNewVersion(action->__writeset[i], action, action->__version);
                w_index = action->__writeset[i].next;
        }
}


inline void MVScheduler::ScheduleTransaction(mv_action *action) {
  if ((action->__combinedHash & txnMask) != 0) {
          ProcessWriteset(action);
  }
}
