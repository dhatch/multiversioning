#include <scheduler.h>
#include <machine.h>
#include <city.h>
#include <catalog.h>
#include <database.h>
#include <action.h>
#include <cpuinfo.h>
#include <sstream>

#include <stdlib.h>

#include <cassert>
#include <cstring>
#include <deque>

using namespace std;

uint32_t MVScheduler::NUM_CC_THREADS = 1;

MVScheduler::MVScheduler(MVSchedulerConfig config) : 
        Runnable(config.cpuNumber) 
{
        std::stringstream msg;
        msg << "Scheduler thread " << config.threadId << " started on cpu " << config.cpuNumber << "\n";
        std::cout << msg.str();

        this->config = config;
        this->epoch = 0;
        this->txnCounter = 0;
        this->txnMask = ((uint64_t)1<<config.threadId);

        this->partitions = 
                (MVTablePartition**)alloc_mem(sizeof(MVTablePartition*)*config.numTables, 
                                              config.cpuNumber);
        assert(this->partitions != NULL);

        /* Initialize the allocator and the partitions. */
        this->alloc = new (config.cpuNumber) MVRecordAllocator(config.allocatorSize, 
                                                               config.cpuNumber,
                                                               config.worker_start,
                                                               config.worker_end);
        for (uint32_t i = 0; i < this->config.numTables; ++i) {

                /* Track the partition locally and add it to the database's catalog. */
                this->partitions[i] =
                        new (config.cpuNumber) MVTablePartition(config.tblPartitionSizes[i],
                                                                config.cpuNumber, alloc);
                assert(this->partitions[i] != NULL);
        }
        this->threadId = config.threadId;
}

static inline uint64_t compute_version(uint32_t epoch, uint32_t txnCounter) {
    return (((uint64_t)epoch << 32) | txnCounter);
}

void MVScheduler::Init () {}

void MVScheduler::StartWorking() 
{
        //  std::cout << config.numRecycleQueues << "\n";
        while (true) {
                ActionBatch curBatch = config.inputQueue->DequeueBlocking();

                for (uint32_t i = 0; i < config.numSubords; ++i) 
                        config.pubQueues[i]->EnqueueBlocking(curBatch);

                mv_action* action = curBatch.actionBuf[0];
                while (true) {
                  ScheduleTransaction(action);
                  int nextAction = action->__nextAction[threadId];
                  if  (nextAction == -1) {
                    break;
                  }
                  action = curBatch.actionBuf[nextAction];
                }

                for (uint32_t i = 0; i < config.numSubords; ++i) 
                        config.subQueues[i]->DequeueBlocking();
                for (uint32_t i = 0; i < config.numOutputs; ++i) 
                        config.outputQueues[i].EnqueueBlocking(curBatch);
                Recycle();
        }
}

void MVScheduler::Recycle() 
{
        /* Check for recycled MVRecords */
        for (uint32_t i = 0; i < config.numRecycleQueues; ++i) {
                MVRecordList recycled;
                while (config.recycleQueues[i]->Dequeue(&recycled)) {
                        //      std::cout << "Received recycled mv records: " << recycled.count << "\n";
                        this->alloc->ReturnMVRecords(recycled);
                }
        }  
}

/*
 * For each record in the writeset, write out a placeholder indicating that
 * the value for the record will be produced by this transaction. We don't need
 * to track the version of each record written by the transaction. The version
 * is equal to the transaction's timestamp.
 */
inline void MVScheduler::ScheduleTransaction(mv_action *action) 
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

