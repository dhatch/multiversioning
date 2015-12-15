#include <executor.h>
#include <algorithm>

extern uint32_t GLOBAL_RECORD_SIZE;

PendingActionList::PendingActionList(uint32_t freeListSize) {
  freeList = (ActionListNode*)malloc(sizeof(ActionListNode)*freeListSize);
  memset(freeList, 0x00, sizeof(ActionListNode)*freeListSize);
  
  for (uint32_t i = 0; i < freeListSize; ++i) {
    freeList[i].next = &freeList[i+1];
  }
  freeList[freeListSize-1].next = NULL;
  this->head = NULL;
  this->tail = NULL;
  this->cursor = NULL;
  this->size = 0;
}

inline void PendingActionList::EnqueuePending(mv_action *action) {
  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));

  assert(freeList != NULL);
  assert(action != NULL);

  ActionListNode *node = freeList;
  freeList = freeList->next;  
  node->action = action;
  node->next = NULL;  
  node->prev = NULL;

  if (tail == NULL) {
    assert(head == NULL && size == 0);
    node->prev = NULL;
    head = node;  
    tail = node;
  }
  else {
    assert(head != NULL && size > 0);
    tail->next = node;
    node->prev = tail;
  }
  this->size += 1;
  tail = node;

  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::DequeuePending(ActionListNode *node) {
  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
  assert(node != cursor);
  assert(size > 0);

  if (node->next == NULL) {
    tail = node->prev;
  }
  else {
    node->next->prev = node->prev;
  }
  
  if (node->prev == NULL) {
    head = node->next;
  }
  else {
    node->prev->next = node->next;
  }
  
  /*
  if (node->next == NULL && node->prev == NULL) {
    head = NULL;
    tail = NULL;
  }
  else if (node->next == NULL) {
    node->prev->next = NULL;
    tail = node->prev;
  }
  else if (node->prev == NULL) {
    node->next->prev = NULL;
    head = node->next;
  }
  else {
    node->prev->next = node->next;
    node->next->prev = node->prev;
  }
  */

  node->prev = NULL;
  node->action = NULL;
  node->next = freeList;
  freeList = node;
  this->size -= 1;

  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));
}

inline void PendingActionList::ResetCursor() {
  cursor = head;
}

inline ActionListNode* PendingActionList::GetNext() {
  ActionListNode *temp = cursor;
  if (cursor != NULL) {
    cursor = cursor->next;
  }
  return temp;
}

inline bool PendingActionList::IsEmpty() {
  assert((head == NULL && size == 0) || (head != NULL && size > 0));
  return head == NULL;
}

inline uint32_t PendingActionList::Size() {
  return this->size;
}

Executor::Executor(ExecutorConfig cfg) : Runnable (cfg.cpu) {        
  this->config = cfg;
    this->counter = 0;
  /*
  this->allocators = 

    (RecordAllocator**)malloc(sizeof(RecordAllocator*)*config.numTables);
  for (uint32_t i = 0; i < config.numTables; ++i) {
    uint32_t recSize = config.recordSizes[i];
    uint32_t allocSize = config.allocatorSizes[i];
    this->allocators[i] = 
      new (config.cpu) RecordAllocator(recSize, allocSize, config.cpu);
  }
    */
  this->pendingList = new (config.cpu) PendingActionList(1000);
  this->garbageBin = new (config.cpu) GarbageBin(config.garbageConfig);
  //  this->pendingGC = new (config.cpu) PendingActionList(20000);
  /*
  char *temp_bufs = (char*)alloc_mem(1000*250000, config.cpu);
  memset(temp_bufs, 0x0, 1000*250000);
  this->bufs = (void**)alloc_mem(sizeof(void*)*250000, config.cpu);
  for (uint32_t i = 0; i < 250000; ++i)
          this->bufs[i] = &temp_bufs[1000*i];
  std::random_shuffle(&this->bufs[0], &this->bufs[250000-1]);
  this->buf_ptr = 0;
  */

}

void Executor::Init() {
}

uint64_t Executor::next_ptr()
{
        uint64_t ret = buf_ptr;
        buf_ptr = (buf_ptr + 1) % 250000;
        return ret;
}

void Executor::LeaderFunction() {
  assert(config.threadId == 0);
  //  ActionBatch dummy = { NULL, 0 };
  volatile uint32_t minEpoch = *config.epochPtr;
  //        std::cout << "0:" << minEpoch << "\n";
  for (uint32_t i = 1; i < config.numExecutors; ++i) {
    barrier();
    volatile uint32_t temp = config.epochPtr[i];
    //          std::cout << i << ":" << temp << "\n";
    barrier();
        
    if (temp < minEpoch) {
      minEpoch = temp;
    }
  }
      
  uint32_t prev;
  barrier();
  prev = *config.lowWaterMarkPtr;
  if (minEpoch > prev) {
    *config.lowWaterMarkPtr = minEpoch;
    //          std::cout << "LowWaterMark: " << minEpoch << "\n";
  }
  barrier();

  /*
  for (uint32_t i = 0; i < minEpoch - prev; ++i) {
    config.outputQueue->EnqueueBlocking(dummy);
  }
  */
}
/*
static void wait() 
{
        volatile uint64_t temp = 1;
        while (temp == 1)
                ;
}
*/
void Executor::StartWorking() {
  uint32_t epoch = 1;

  //  ActionBatch dummy;
  while (true) {
    // Process the new batch of transactions
    
    if (config.threadId == 0) {
      ActionBatch batch;
      while (!config.inputQueue->Dequeue(&batch)) {
        LeaderFunction();
      }
      ProcessBatch(batch);
    }
    else {
      ActionBatch batch = config.inputQueue->DequeueBlocking();    
      ProcessBatch(batch);    
    }

    barrier();
    *config.epochPtr = epoch;
    barrier();
    
    /*
    if (DoPendingGC()) {
      // Tell other threads that this epoch is finished
      barrier();
      *config.epochPtr = epoch;
      barrier();
    }
    */
    
    // If this is the leader thread, try to advance the low-water mark to 
    // trigger garbage collection
    if (config.threadId == 0) {
      LeaderFunction();
    }


    // Try to return records that are no longer visible to their owners
    garbageBin->FinishEpoch(epoch);
    
    // Check if there's any garbage we can recycle. Insert into our record 
    // allocators.
    //    RecycleData();

    epoch += 1;
  }

}
/*
uint64_t GetEpoch()
{
        return *config.epochPtr;
}
*/
// Check if other worker threads have returned data to be recycled.
void Executor::RecycleData() {
  uint32_t numTables = config.numTables;
  uint32_t numQueues = config.numQueuesPerTable;
  for (uint32_t i = 0; i < numTables; ++i) {
    for (uint32_t j = 0; j < numQueues; ++j) {
      RecordList recycled;
      
      // Use non-blocking dequeue
      while (config.recycleQueues[i*numQueues+j].Dequeue(&recycled)) {
        //        std::cout << "Received " << recycled.count << " records\n";
        allocators[i]->Recycle(recycled);
      }
    }
  }
}

void Executor::ExecPending() {
  pendingList->ResetCursor();
  for (ActionListNode *node = pendingList->GetNext(); node != NULL; 
       node = pendingList->GetNext()) {
    if (ProcessSingle(node->action)) {
      pendingList->DequeuePending(node);
    }
  }
}

void Executor::ProcessBatch(const ActionBatch &batch) {
        /*
        uint32_t div = batch.numActions/(uint32_t)config.numExecutors;
        uint32_t remainder = batch.numActions % (uint32_t)config.numExecutors;
        uint32_t start = div*config.threadId;
        uint32_t end = div*(config.threadId+1);
        if (config.threadId == config.numExecutors-1) {
                end += remainder;
        }
        for (uint32_t i = end; i >= start; --i) {
                
        }
        */
        for (int i = config.threadId; i < (int)batch.numActions;
             i += config.numExecutors) {
                while (pendingList->Size() > 0) {
                        ExecPending();
                }

                mv_action *cur = batch.actionBuf[i];
                if (!ProcessSingle(cur)) {
                        pendingList->EnqueuePending(cur);
                }
        }

  // DEBUGGIN
  /*
  pendingList->ResetCursor();
  for (ActionListNode *node = pendingList->GetNext(); node != NULL;
       node = pendingList->GetNext()) {
    assert(node->action != NULL);
  }
  */

  while (!pendingList->IsEmpty()) {
    ExecPending();
  }

  ActionBatch dummy = {NULL, 0};
  config.outputQueue->EnqueueBlocking(dummy);  
}

// Returns the epoch of the oldest pending record.
uint32_t Executor::DoPendingGC() {
        static uint64_t mask = 0xFFFFFFFF00000000;
  pendingGC->ResetCursor();
  for (ActionListNode *node = pendingGC->GetNext(); node != NULL; 
       node = pendingGC->GetNext()) {
    assert(node->action != NULL);
    if (ProcessSingleGC(node->action)) {      
      pendingGC->DequeuePending(node);
    }
  }

  if (pendingGC->IsEmpty()) {
    return 0xFFFFFFFF;
  }
  else {
    pendingGC->ResetCursor();
    ActionListNode *node = pendingGC->GetNext();
    assert(node != NULL && node->action != NULL);
    return ((node->action->__version & mask) >> 32);
  }
}

bool Executor::ProcessSingleGC(mv_action *action) {
  assert(action->__state == SUBSTANTIATED);
  uint32_t numWrites = action->__writeset.size();
  bool ret = true;

  // Garbage collect the previous versions
  for (uint32_t i = 0; i < numWrites; ++i) {
    MVRecord *previous = action->__writeset[i].value->recordLink;
    if (previous != NULL) {
      ret &= (previous->writer == NULL || 
              previous->writer->__state == SUBSTANTIATED);
    }
  }
  
  if (ret) {
    for (uint32_t i = 0; i < numWrites; ++i) {
      MVRecord *previous = action->__writeset[i].value->recordLink;
      if (previous != NULL) {
        
        // Since the previous txn has been substantiated, the record's value 
        // shouldn't be NULL.
        assert(previous->value != NULL);
        garbageBin->AddRecord(previous->writingThread, 
                              action->__writeset[i].tableId,
                              previous->value);
        //        garbageBin->AddMVRecord(action->__writeset[i].threadId, previous);
      }
    }
  }
  
  return ret;
}

bool Executor::ProcessSingle(mv_action *action) {
        assert(action != NULL);
        volatile uint64_t state;
        barrier();
        state = action->__state;
        barrier();
        if (state != SUBSTANTIATED) {
                if (state == STICKY &&
                    cmp_and_swap(&action->__state, STICKY, PROCESSING)) {
                        if (ProcessTxn(action)) {
                                return true;
                        } else {
                                xchgq(&action->__state, STICKY);
                                return false;
                        }
                } else {      // cmp_and_swap failed
                        return false;
                }
        }
        else {        // action->state == SUBSTANTIATED
                return true;
        }
}

bool Executor::check_ready(mv_action *action)
{
        uint32_t num_reads, num_writes, i;
        bool ready;
        mv_action *depend_action;
        MVRecord *prev;
        void *new_data, *old_data;
        uint32_t *read_index, *write_index;

        ready = true;
        num_reads = action->__readset.size();
        num_writes = action->__writeset.size();
        read_index = &action->read_index;
        write_index = &action->write_index;
        for (; *read_index < num_reads; *read_index += 1) {
                i = *read_index;

                assert(action->__readset[i].value != NULL);
                depend_action = action->__readset[i].value->writer;
                if (depend_action != NULL &&
                    depend_action->__state != SUBSTANTIATED &&
                    !ProcessSingle(depend_action)) {
                        ready = false;
                        break;
                }
        }
        for (; *write_index < num_writes; *write_index += 1) {
                i = *write_index;
                assert(action->__writeset[i].value != NULL);
                if (action->__writeset[i].is_rmw) {
                        prev = action->__writeset[i].value->recordLink;
                        assert(prev != NULL);
                        depend_action = prev->writer;
                        if (depend_action != NULL &&
                            depend_action->__state != SUBSTANTIATED && 
                            !ProcessSingle(depend_action)) {
                                ready = false;
                                break;
                        } else if (action->__writeset[i].initialized == false) {
                                /* 
                                 * XXX This is super hacky. Need to separate 
                                 * record allocation from version allocation to 
                                 * make it work -- "engineering work". 
                                 */
                                new_data = action->__writeset[i].value->value;
                                old_data = prev->value;
                                memcpy(new_data, old_data, GLOBAL_RECORD_SIZE);
                                action->__writeset[i].initialized = true;
                        }
                }
        }
        return ready;
}

bool Executor::ProcessTxn(mv_action *action) {
        assert(action != NULL && action->__state == PROCESSING);
        if (action->__readonly == true) {
                assert(action->__writeset.size() == 0);
                uint32_t num_reads = action->__readset.size();
                uint64_t action_epoch = GET_MV_EPOCH(action->__version);
                for (uint32_t i = 0; i < num_reads; ++i) {
                        MVRecord *rec = action->__readset[i].value;
                        MVRecord *snapshot;
                  if (action_epoch == GET_MV_EPOCH(rec->createTimestamp)) {
                          snapshot = rec->epoch_ancestor;
                  } else {
                          snapshot = rec;
                  }
                  barrier();
                  void *value_ptr = snapshot->value;
                  barrier();
                  if (value_ptr == NULL)
                          return false;
                }
                action->exec = this;
                action->Run();
                xchgq(&action->__state, SUBSTANTIATED);
                return true;
        }
        uint32_t numWrites = action->__writeset.size();
        bool ready = check_ready(action);
        if (ready == false) {
                return false;
        }
        /*else {
                for (uint32_t i = 0; i < numWrites; ++i) {
                        uint64_t index = next_ptr();
                        void *buf = this->bufs[index];
                        action->__writeset[i].value->value = (Record*)buf;
                }
        }
        */

        
        /*
        bool ready = check_ready(action);
        if (!ready) {
                return false;
        }
        */
        
        /*
          for (uint32_t i = 0; i < numWrites; ++i) {
          uint32_t tbl = action->__writeset[i].tableId;
          Record **valuePtr = &action->__writeset[i].value->value;
          action->__writeset[i].value->writingThread = config.threadId;
          bool success = allocators[tbl]->GetRecord(valuePtr);
          assert(success);
          }
        */
  
  // Transaction aborted
        action->exec = this;
        action->Run();
        /*
  if (!action->Run()) {
    assert(false);
    for (uint32_t i = 0; i < numWrites; ++i) {
      uint32_t tbl = action->__writeset[i].tableId;
      uint32_t recSize = config.recordSizes[tbl];
      Record *curValuePtr = action->__writeset[i].value->value;
      Record *prevValuePtr = NULL;
      MVRecord *predecessor = action->__writeset[i].value->recordLink;
      if (predecessor != NULL) {
        prevValuePtr = predecessor->value;
      }
      
      if (prevValuePtr != NULL) {
        memcpy(curValuePtr, prevValuePtr, sizeof(uint64_t)+recSize);
      }
    }    
  }
        */

        barrier();
        xchgq(&action->__state, SUBSTANTIATED);
        barrier();

  for (uint32_t i = 0; i < numWrites; ++i) {
          //          xchgq((volatile uint64_t*)&action->__writeset[i].value->writer,
          //                (uint64_t)NULL);
          //          action->__writeset[i].value->writer = NULL;
  
    MVRecord *previous = action->__writeset[i].value->recordLink;
    if (previous != NULL) {
      garbageBin->AddMVRecord(action->__writeset[i].threadId, previous);
    }

  }

  //  bool gcSuccess = ProcessSingleGC(action);
  //  assert(gcSuccess);
  /*
  for (uint32_t i = 0; i < numWrites; ++i) {
    action->__writeset[i].value->writer = NULL;        
  }
  */
  assert(ready);
  return ready;
}

GarbageBin::GarbageBin(GarbageBinConfig config) {
  assert(sizeof(MVRecordList) == sizeof(RecordList));
  this->config = config;
  this->snapshotEpoch = 0;

  // total number of structs
  uint32_t numStructs = 
    (config.numCCThreads + config.numWorkers*config.numTables);
  uint32_t ccOffset = config.numCCThreads*sizeof(MVRecordList);
  uint32_t workerOffset = 
    config.numWorkers*config.numTables*sizeof(MVRecordList);

  // twice #structs: one for live, one for snapshot
  void *data = alloc_mem(2*numStructs*sizeof(MVRecordList), config.cpu);
  memset(data, 0x00, 2*numStructs*sizeof(MVRecordList));
  
  this->curStickies = (MVRecordList*)data;
  this->snapshotStickies = (MVRecordList*)((char*)data + ccOffset);
  for (uint32_t i = 0; i < 2*config.numCCThreads; ++i) {
    curStickies[i].tail = &curStickies[i].head;
    curStickies[i].head = NULL;
    curStickies[i].count = 0;
  }
  
  this->curRecords = (RecordList*)((char*)data + 2*ccOffset);
  this->snapshotRecords = (RecordList*)((char*)data + 2*ccOffset+workerOffset);
  for (uint32_t i = 0; i < 2*config.numWorkers; ++i) {
    curRecords[i].tail = &curRecords[i].head;
    curRecords[i].head = NULL;
    curRecords[i].count = 0;
  }
}

void GarbageBin::AddMVRecord(uint32_t ccThread, MVRecord *rec) {
  rec->allocLink = NULL;
  // rec->allocLink = NULL;
  *(curStickies[ccThread].tail) = rec;
  curStickies[ccThread].tail = &rec->allocLink;
  curStickies[ccThread].count += 1;
  assert(curStickies[ccThread].head != NULL);
}

void GarbageBin::AddRecord(uint32_t workerThread, uint32_t tableId, 
                           Record *rec) {
  //  rec->next = NULL;
  *(curRecords[workerThread*config.numTables+tableId].tail) = rec;
  curRecords[workerThread*config.numTables+tableId].tail = &rec->next;  
  curRecords[workerThread*config.numTables+tableId].count += 1;
}

void GarbageBin::ReturnGarbage() {
  for (uint32_t i = 0; i < config.numCCThreads; ++i) {
    // *curStickies[i].tail = NULL;

    // Try to enqueue garbage. If enqueue fails, we'll just try again during the
    // next call.
    if (snapshotStickies[i].head != NULL) {
      if (!config.ccChannels[i]->Enqueue(snapshotStickies[i])) {
        *(curStickies[i].tail) = snapshotStickies[i].head;
        curStickies[i].tail = snapshotStickies[i].tail;
        curStickies[i].count += snapshotStickies[i].count;
      }
      else {
        //        std::cout << "Recycle!\n";
      }
    }
    snapshotStickies[i] = curStickies[i];
    curStickies[i].head = NULL;
    curStickies[i].tail = &curStickies[i].head;
    curStickies[i].count = 0;
  }
  /*
  uint32_t tblCount = config.numTables;
  for (uint32_t i = 0; i < config.numWorkers; ++i) {
    for (uint32_t j = 0; j < tblCount; ++j) {      
      uint32_t index = i*tblCount + j;

      // Same logic as "stickies"
      if (snapshotRecords[index].head != NULL) {
        if (!config.workerChannels[index]->Enqueue(snapshotRecords[index])) {
          *(curRecords[index].tail) = snapshotRecords[index].head;
          curRecords[index].tail = snapshotRecords[index].tail;
          curRecords[index].count += snapshotRecords[index].count;
        }
      }
      snapshotRecords[index] = curRecords[index];
      curRecords[index].head = NULL;
      curRecords[index].tail = &curRecords[index].head;
      curRecords[index].count = 0;
    }
  }
  */
  //  std::cout << "Success!\n";
}

void GarbageBin::FinishEpoch(uint32_t epoch) {
  barrier();
  uint32_t lowWatermark = *config.lowWaterMarkPtr;
  barrier();
  
  if (lowWatermark >= snapshotEpoch) {
    ReturnGarbage();
    snapshotEpoch = epoch;
    //    std::cout << "Success: " << epoch << "\n";
  }
}

RecordAllocator::RecordAllocator(size_t recordSize, uint32_t numRecords, 
                                 int cpu) {
  char *data = 
    (char*)alloc_mem(numRecords*(sizeof(Record)+recordSize), cpu);
  memset(data, 0x00, numRecords*(sizeof(Record)+recordSize));
  for (uint32_t i = 0; i < numRecords; ++i) {
    ((Record*)(data + i*(sizeof(Record)+recordSize)))->next = 
      (Record*)(data + (i+1)*(sizeof(Record)+recordSize));
  }
  ((Record*)(data + (numRecords-1)*(sizeof(Record)+recordSize)))->next = NULL;
  //  data[numRecords-1].next = NULL;
  freeList = (Record*)data;
}

bool RecordAllocator::GetRecord(Record **OUT_REC) {
  if (freeList != NULL) {
    Record *temp = freeList;
    freeList = freeList->next;
    temp->next = NULL;
    *OUT_REC = temp;    
    return true;
  }
  else {
    *OUT_REC = NULL;
    return false;
  }
}

void RecordAllocator::FreeSingle(Record *rec) {
  rec->next = freeList;
  freeList = rec;
}

void RecordAllocator::Recycle(RecordList recList) {
  *(recList.tail) = freeList;
  freeList = recList.head;
}
