#include <executor.h>

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

inline void PendingActionList::EnqueuePending(Action *action) {
  assert((head != NULL && tail != NULL && size > 0) || 
         (head == NULL && tail == NULL && size == 0));

  assert(freeList != NULL);
  assert(action != NULL);
  assert(size >= 0);

  assert(this->seen.find(action) == this->seen.end());
  this->seen.insert(action);
  assert(this->seen.find(action) != this->seen.end());

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
}

void Executor::Init() {
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
}

void Executor::LeaderFunction() {
  assert(config.threadId == 0);
  ActionBatch dummy;
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
      
  for (uint32_t i = 0; i < minEpoch - prev; ++i) {
    config.outputQueue->EnqueueBlocking(dummy);
  }
}

void Executor::StartWorking() {
  uint32_t epoch = 1;
  ActionBatch dummy;
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
        for (uint32_t i = config.threadId; i < batch.numActions; i += config.numExecutors) {
                Action *cur = batch.actionBuf[i];
                ProcessSingle(cur);
        }
        /*
  for (uint32_t i = batch.numActions-1-config.threadId; i < batch.numActions;
       i -= config.numExecutors) {
    while (pendingList->Size() > 10) {
      ExecPending();
    }

    Action *cur = batch.actionBuf[i];
    if (!ProcessSingle(cur)) {
      pendingList->EnqueuePending(cur);
    }
  }
        */
        
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
}

static uint32_t GetEpoch(Action *action) {
  static uint64_t mask = 0xFFFFFFFF00000000;
  return ((action->version & mask) >> 32);
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
    return ((node->action->version & mask) >> 32);
  }
}

bool Executor::ProcessSingleGC(Action *action) {
  assert(action->state == SUBSTANTIATED);
  uint32_t numWrites = action->writeset.size();
  bool ret = true;

  // Garbage collect the previous versions
  for (uint32_t i = 0; i < numWrites; ++i) {
    MVRecord *previous = action->writeset[i].value->recordLink;
    if (previous != NULL) {
      ret &= (previous->writer == NULL || 
              previous->writer->state == SUBSTANTIATED);
    }
  }
  
  if (ret) {
    for (uint32_t i = 0; i < numWrites; ++i) {
      MVRecord *previous = action->writeset[i].value->recordLink;
      if (previous != NULL) {
        
        // Since the previous txn has been substantiated, the record's value 
        // shouldn't be NULL.
        assert(previous->value != NULL);
        garbageBin->AddRecord(previous->writingThread, 
                              action->writeset[i].tableId,
                              previous->value);
        //        garbageBin->AddMVRecord(action->writeset[i].threadId, previous);
      }
    }
  }
  
  return ret;
}

bool Executor::ProcessSingle(Action *action) {
  assert(action != NULL);
  if (action->state != SUBSTANTIATED) {
    if (cmp_and_swap(&action->state, STICKY, PROCESSING)) {
      if (ProcessTxn(action)) {
        return true;
      }
      else {
        barrier();
        action->state = STICKY;
        barrier();
        return false;
      }
    }
    else {      // cmp_and_swap failed
      return false;
    }
  }
  else {        // action->state == SUBSTANTIATED
    return true;
  }
}

bool Executor::ProcessTxn(Action *action) {
  assert(action != NULL && action->state == PROCESSING);
  bool ready = true;
  bool abort = false;
  uint32_t numReads = action->readset.size();
  uint32_t numWrites = action->writeset.size();

  // First ensure that all transactions on which the current one depends on have
  // been processed.
  for (size_t i = 0; i < numReads; ++i) {
    /*
    if (action->readset[i].value == NULL) {
      CompositeKey curKey = action->readset[i];
      MVRecord *record = 
        DB.GetTable(curKey.tableId)->GetMVRecord(curKey.threadId, curKey, 
                                                 action->version);
      // If the record does not exist, abort.
      if (record == NULL) {
        abort = true;
      }
    
      // If this read is part of an RMW, then we need to read the previous 
      // version of the record
      if (record->writer == action) {
        record = record->recordLink;
        if (record == NULL) {
          abort = true;
        }
      }
      // Keep a reference to the record
      action->readset[i].value = record;
    }
    */

    if (action->readset[i].value != NULL) {
      // Check that the txn which is supposed to have produced the value of the 
      // record has been executed.
            while (true) {
                    Action *dependAction = action->readset[i].value->writer;
                    if (dependAction != NULL && !ProcessSingle(dependAction)) 
                            continue;
                    else
                            break;
      }
    }
  }   

  for (size_t i = 0; i < numWrites; ++i) {
    // Keep a reference to the sticky we need to evaluate. 
    if (action->writeset[i].value == NULL) {
      
      // Find the sticky
      CompositeKey curKey = action->writeset[i];
      MVRecord *record = 
        DB.GetTable(curKey.tableId)->GetMVRecord(curKey.threadId, curKey, 
                                                 action->version);

      // Sticky better exist
      assert(record != NULL);
      action->writeset[i].value = record;      
    }

    if (action->writeset[i].is_rmw) {
            while (true) {
                    Action *prevAction = action->writeset[i].value->recordLink->writer;
                    if (prevAction != NULL && !ProcessSingle(prevAction)) {
                            continue;
                    } else {
                            break;
                    }
            }
    }
    
    // Ensure that the previous version of this record has been written
    /*
    MVRecord *prev = action->writeset[i].value->recordLink;
    if (prev != NULL) {
      // There exists a previous version
      Action *dependAction = prev->writer;
      if (dependAction != NULL && !ProcessSingle(dependAction)) {
        ready = false;
      }
    }
    */
  }
  
  // If abort is true at this point, it's because the txn tried to read a 
  // non-existent record
  if (!ready) {
    return false;
  }

  /*
  for (uint32_t i = 0; i < numWrites; ++i) {
    uint32_t tbl = action->writeset[i].tableId;
    Record **valuePtr = &action->writeset[i].value->value;
    action->writeset[i].value->writingThread = config.threadId;
    bool success = allocators[tbl]->GetRecord(valuePtr);
    assert(success);
  }
  */
  
  // Transaction aborted
  if (abort || !action->Run()) {
    assert(false);
    for (uint32_t i = 0; i < numWrites; ++i) {
      uint32_t tbl = action->writeset[i].tableId;
      uint32_t recSize = config.recordSizes[tbl];
      Record *curValuePtr = action->writeset[i].value->value;
      Record *prevValuePtr = NULL;
      MVRecord *predecessor = action->writeset[i].value->recordLink;
      if (predecessor != NULL) {
        prevValuePtr = predecessor->value;
      }
      
      if (prevValuePtr != NULL) {
        memcpy(curValuePtr, prevValuePtr, sizeof(uint64_t)+recSize);
      }
    }    
  }

  barrier();
  //  xchgq(&action->state, SUBSTANTIATED);
  action->state = SUBSTANTIATED;
  barrier();
  for (uint32_t i = 0; i < numWrites; ++i) {
    action->writeset[i].value->writer = NULL;        
    MVRecord *previous = action->writeset[i].value->recordLink;
    if (previous != NULL) {
        
      // Since the previous txn has been substantiated, the record's value 
      // shouldn't be NULL.
      assert(previous->value != NULL);
      /*
      garbageBin->AddRecord(previous->writingThread, 
                            action->writeset[i].tableId,
                            previous->value);
      */
      garbageBin->AddMVRecord(action->writeset[i].threadId, previous);
    }
  }
  //  bool gcSuccess = ProcessSingleGC(action);
  //  assert(gcSuccess);
  /*
  for (uint32_t i = 0; i < numWrites; ++i) {
    action->writeset[i].value->writer = NULL;        
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
