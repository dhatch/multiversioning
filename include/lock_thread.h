#ifndef         LOCK_THREAD_H_
#define         LOCK_THREAD_H_

#include <lock_manager.h>
#include <concurrent_queue.h>
#include <runnable.hh>

struct LockActionBatch {
  uint64_t batchSize;
  LockingAction **actions;
};

struct LockThreadConfig {
  uint32_t allocatorSize;
  SimpleQueue<LockActionBatch> *inputQueue;
  SimpleQueue<LockActionBatch> *outputQueue;
  LockManager *mgr;
};

class LockThread : public Runnable {
 private:
  BucketEntryAllocator *alloc;
  LockThreadConfig config;

 protected:
  virtual void Init();

  virtual void StartWorking();

 public:
  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }

  LockThread(LockThreadConfig config, int cpu);
};

#endif          // LOCK_THREAD_H_
