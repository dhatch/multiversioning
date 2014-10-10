#include <lock_thread.h>

LockThread::LockThread(LockThreadConfig config, int cpu) : Runnable(cpu) {
  this->config = config;
  alloc = new (cpu) BucketEntryAllocator(config.allocatorSize, cpu);
  assert(alloc != NULL);
}

void LockThread::StartWorking() {
  while (true) {
    LockActionBatch b = config.inputQueue->DequeueBlocking();
    for (uint64_t i = 0; i < b.batchSize; ++i) {
      config.mgr->AcquireLocks(b.actions[i]);
    }
    config.outputQueue->EnqueueBlocking(b);
  }
}

void LockThread::Init() {

}
