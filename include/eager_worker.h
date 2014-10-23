#ifndef                 EAGER_WORKER_HH_
#define                 EAGER_WORKER_HH_

#include <lock_manager.h>
#include <concurrent_queue.h>
#include <pthread.h>
#include <cpuinfo.h>
#include <runnable.hh>

struct EagerActionBatch {
  uint32_t batchSize;
  EagerAction **batch;
};

struct EagerWorkerConfig {
  LockManager *mgr;
  SimpleQueue<EagerActionBatch> *inputQueue;
  SimpleQueue<EagerActionBatch> *outputQueue;  
  int cpu;
  uint32_t maxPending;
};

class EagerWorker : public Runnable {
private:
  EagerWorkerConfig config;
  EagerAction *m_queue_head;                  // Head of queue of waiting txns
  EagerAction *m_queue_tail;                  // Tail of queue of waiting txns
  int         m_num_elems;                    // Number of elements in the queue

  volatile uint32_t m_num_done;
    
  // Worker thread function
  virtual void WorkerFunction();

  void Enqueue(EagerAction *txn);

  void RemoveQueue(EagerAction *txn);

  void CheckReady();

  void TryExec(EagerAction *txn);

  void DoExec(EagerAction *txn);
    
  uint32_t QueueCount(EagerAction *txn);

protected:    
  virtual void StartWorking();
  
  virtual void Init();

public:

  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }

  EagerWorker(EagerWorkerConfig config);
    
  uint32_t NumProcessed() {
    return m_num_done;
  }
};

#endif           // EAGER_WORKER_HH_
