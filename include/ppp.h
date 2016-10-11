#ifndef PPP_H_
#define PPP_H_

#include <mv_action.h>
#include <runnable.hh>
#include <concurrent_queue.h>
#include <numa.h>
#include <mv_table.h>

#define _NUM_PARTITIONS_ 2

extern uint64_t recordSize;

class CompositeKey;
class mv_action;
class MVRecordAllocator;
class MVTablePartition;

/* An MVActionDistributor is the real first stage of the transaction
 * processing pipeline (meant to replace MVActionHasher)
 * Its job is to take incoming batches and assign transactions to a
 * concurrency control worker thread (or virtual partition)
 *
 * The number of virtual partitions is fixed for now
 */

class MVActionDistributor : public Runnable {
  private:

    SimpleQueue<ActionBatch> *inputQueue;

    SimpleQueue<ActionBatch> *outputQueues[_NUM_PARTITIONS_];

  protected:

    virtual void StartWorking();
    virtual void Init();
    static inline void ProcessAction(mv_action *action, uint32_t epoch, 
                                     uint32_t txnCounter);

  public:
    void* operator new(std::size_t sz, int cpu);

    MVActionDistributor(int cpuNumber,
        SimpleQueue<ActionBatch> *inputQueue,
        SimpleQueue<ActionBatch> *outputQueue[]);
};

#endif    /* PPP_H_ */
