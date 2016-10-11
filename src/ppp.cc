#include <ppp.h>

void* MVActionDistributor::operator new(std::size_t sz, int cpu) {
  void *ret = alloc_mem(sz, cpu);
  assert(ret != NULL);
  return ret;
}

void MVActionDistributor::Init() {}

MVActionDistributor::MVActionDistributor(int cpuNumber, 
    SimpleQueue<ActionBatch> *inputQueue,
    SimpleQueue<ActionBatch> *outputQueues[]
): Runnable(cpuNumber) {
  this->inputQueue = inputQueue;
  this->outputQueues = outputQueues;
}

void MVActionDistributor::StartWorking() {
  uint32_t epoch = 0;
  while (true) {
    // Take a batch from input...
    ActionBatch batch = inputQueue->DequeueBlocking();
    uint32_t numActions = batch.numActions;
    // Build batches as they come in for each partition?
    // Would be easier to just send the composite keys to the queue
    // But obviously that creates a bottleneck on the queue
    // So Somehow have to create a new batch obect, whether its txns
    // or queues
    // o
  }



}
