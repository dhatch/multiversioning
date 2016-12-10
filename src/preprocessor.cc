#include <preprocessor.h>

uint32_t MVActionDistributor::NUM_CC_THREADS = 1;

void MVActionDistributor::log (string msg) {
  std::stringstream m;
  m << config.label << ": " << msg << "\n";
  std::cout << m.str();
}
  
void* MVActionDistributor::operator new(std::size_t sz, int cpu) {
  void *ret = alloc_mem(sz, cpu);
  assert(ret != NULL);
  return ret;
}

void MVActionDistributor::Init() {}

MVActionDistributor::MVActionDistributor(MVActionDistributorConfig config) :
  Runnable(config.cpuNumber) {
  std::stringstream msg;
  msg << "Preprocessor thread " << config.threadId << " started on cpu " << config.cpuNumber << "\n";
  std::cout << msg.str();
  
  this->config = config;
  this->leader = config.subQueues != NULL;

}

/*
 * Hash the given key, and find which concurrency control thread is
 * responsible for the appropriate key range. 
 */
uint32_t MVActionDistributor::GetCCThread(CompositeKey& key) 
{
        uint64_t hash = CompositeKey::Hash(&key);
        return (uint32_t)(hash % NUM_CC_THREADS);
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
void MVActionDistributor::ProcessAction(mv_action * action, int* last_actions, mv_action ** batch, int index) {
  int cc_threads[NUM_CC_THREADS] = {-1};
  int keys[NUM_CC_THREADS];

  for (int i = 0; i < NUM_CC_THREADS; i++) {
    keys[i] = -1;
  }

  for(int i = 0; i < action->__readset.size(); i++) {
    int partition = GetCCThread(action->__readset[i]);
    cc_threads[partition] = 1;
    int index = keys[partition];
    if (index == -1) {
      action->__read_starts[partition] = i;
    } else {
      action->__readset[index].next = i;
    }
    keys[partition] = i;
  }

  for (int i = 0; i < NUM_CC_THREADS; i++) {
    keys[i] = -1;
  }

  for(int i = 0; i < action->__writeset.size(); i++) {
    int partition = GetCCThread(action->__writeset[i]);
    cc_threads[partition] = 1;
    int index = keys[partition];
    if (index != -1) {
      action->__writeset[index].next = i;
    } else {
      action->__write_starts[partition] = i;
    }
    keys[partition] = i;
  }

  for(int i = 0; i < NUM_CC_THREADS; i++) {
    if (cc_threads[i] == 1) {
      batch[last_actions[i]]->__nextAction[i] = index;
      last_actions[i] = index;
    }
  }
}

void MVActionDistributor::StartWorking() {
  //uint32_t epoch = 0;
  if (leader) {
    log("Leader thread started!");
    int pubindex = 0;
    int subindex = 0;
    while (true) {

      // See if there is a batch available from input...
      ActionBatch batch;
      if (config.inputQueue->Dequeue(&batch)) {
        // Send it round robin to the subordinate threads
        config.pubQueues[pubindex]->EnqueueBlocking(batch);
        pubindex = (pubindex + 1) % config.numSubords;
      }

      // See if there is a batch available from subordinates...
      ActionBatch outBatch;
      if (config.subQueues[subindex]->Dequeue(&outBatch)) {
        config.outputQueue->EnqueueBlocking(outBatch);
        subindex = (subindex + 1) % config.numSubords;
      }
    }
  } else {
    log("Subordinate thread started!");
    while (true) {
      ActionBatch batch = config.inputQueue->DequeueBlocking();
      mv_action** actions = batch.actionBuf;
      uint32_t numActions = batch.numActions;

      // Allocate the output batches here for now as linked lists
      int lastActions[NUM_CC_THREADS] = {0};

      // Pre process each txn
      for (uint32_t i = 0; i < numActions; ++i) {
        mv_action * action = actions[i];
        ProcessAction(action, lastActions, batch.actionBuf, i);
      }

      // Ensure the last action in the batch has negative nextAction values
      mv_action* action = actions[numActions - 1];
      for (uint32_t i = 0 ; i < NUM_CC_THREADS; i++) {
        action->__nextAction[i] = -1;
      }

      config.outputQueue->EnqueueBlocking(batch);
    }
  }
}
 


