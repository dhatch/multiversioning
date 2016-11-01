#include <ppp.h>

void MVActionDistributor::log (string msg) {
  std::stringstream m;
  m << this->getCpuNum() << ": " << msg << "\n";
  std::cout << m.str();
}

void* MVActionDistributor::operator new(std::size_t sz, int cpu) {
  void *ret = alloc_mem(sz, cpu);
  assert(ret != NULL);
  return ret;
}

void MVActionDistributor::Init() {}

MVActionDistributor::MVActionDistributor(int cpuNumber, 
    SimpleQueue<ActionBatch> *inputQueue,
    SimpleQueue<CompositeKey*> **outputQueues,
    SimpleQueue<int> *orderInput,
    SimpleQueue<int> *orderOutput,
    bool leader
): Runnable(cpuNumber) {
  this->inputQueue = inputQueue;
  this->outputQueues = outputQueues;
  this->orderingInputQueue = orderInput;
  this->orderingOutputQueue = orderOutput;
  // If this is the leader preprocessing thread (the first one),
  // pre-empt the input queue so that operation doesn't get blocked
  // on the first batch
  if (leader) {
    orderInput->EnqueueBlocking(1);
  }
}

/*
 * Hash the given key, and find which concurrency control thread is
 * responsible for the appropriate key range. 
 */
uint32_t MVActionDistributor::GetCCThread(CompositeKey key) 
{
        uint64_t hash = CompositeKey::Hash(&key);
        return (uint32_t)(hash % _NUM_PARTITIONS_);
}

void MVActionDistributor::ProcessKeySet(std::vector<CompositeKey> set, CompositeKey ** heads, CompositeKey ** tails) {
      for (std::vector<CompositeKey>::iterator it = set.begin() ; it != set.end(); ++it) {
        // Find the appropriate partition
        std::stringstream msg;
        msg << "Examining record " << it->key << " placing in partition" << partition;
        log(msg.str());
        if (heads[partition] == NULL) {
          heads[partition] = &(*it);
          tails[partition] = &(*it);
        } else {
          tails[partition]->next_key = &(*it);
          tails[partition] = &(*it);
        }
      }
}

// Output to concurrency control layer:
// Batch of txns that are guaranteed to contain read/write elements that 
// are relevant to the thread
//
// List of ints that refer to the index within the writeset/readset of 
// the specific composite keys that we care about, ordered in the same
// order as the batch

// Constructing a new thing to pass: the txn is included but can it be modified?
// Why not? 
void MVActionDistributor::ProcessAction(mv_action * action, mv_action * last_action) {
  std::vector<CompositeKey> readset = action->__readset;
  std::vector<CompositeKey> writeset = action->__writeset;
  int cc_threads[_NUM_PARTITIONS_] = {0};
  int keys[_NUM_PARTITIONS_];

  for (int i = 0; i < _NUM_PARTITIONS_; i++) {
    keys[i] = -1;
  }
  for(int i = 0; i < readset.size(); i++) {
    int partition = GetCCThread(*it);
    cc_threads[partition] = 1;
    int index = keys[partition];
    if (index != -1) {
      readset[index].next = i;
    } else {
      action->__read_starts[partition] = i;
    }
    keys[partition] = i;
  }

  for (int i = 0; i < _NUM_PARTITIONS_; i++) {
    keys[i] = -1;
  }
  for(int i = 0; i < write_set.size(); i++) {
    int partition = GetCCThread(*it);
    cc_threads[partition] = 1;
    int index = keys[partition];
    int index = keys[partition];
    if (index != -1) {
      readset[index].next = i;
    } else {
      action->__writestarts[partition] = i;
    }
    keys[partition] = i;
  }

  if (last_action != NULL) {
    for(int i = 0; i < _NUM_PARTITIONS_; i++) {
      int partition = cc_threads[i];
      last_action->__nextAction[partition] = action;
    }
  }
}

void MVActionDistributor::StartWorking() {
  //uint32_t epoch = 0;
  log("Thread started!");
  while (true) {
    // Take a batch from input...
    ActionBatch batch = inputQueue->DequeueBlocking();
    mv_action** actions = batch.actionBuf;
    uint32_t numActions = batch.numActions;
    // Allocate the output batches here for now as linked lists

    // Pre process each txn
    for(uint32_t i = 0; i < numActions; ++i) {
      mv_action * action = actions[i];
      mv_action * last_action;
      if (i == 0) {
        last_action = NULL,
      } else {
        last_action = actions[i - 1];
      }
      ProcessAction(action, last_action);
    }
    // Possible design for interthread comms
    // Queue between threads in round robin
    // Wait until previouus thread has told us we can output
    // At a later point perhaps we can make it more dynamic and 
    // begin working on the next batch while waiting
    orderingInputQueue->DequeueBlocking();
    // do the output
    for(uint32_t i = 0; i < _NUM_PARTITIONS_; ++i) {
      outputQueues[i]->EnqueueBlocking(batch);
    }
    // Notify next thread that they can output
    orderingOutputQueue->EnqueueBlocking(1);

  }



}
