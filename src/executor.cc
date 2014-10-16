#include <executor.h>

PendingActionList::PendingActionList(uint32_t freeListSize) {
  freeList = (ActionListNode*)malloc(sizeof(ActionListNode)*freeListSize);
  memset(freeList, 0x00, sizeof(ActionListNode)*freeListSize);
  
  for (uint32_t i = 0; i < freeListSize; ++i) {
    freeList[i].next = &freeList[i+1];
  }
  freeList[freeListSize-1].next = NULL;
}

inline void PendingActionList::EnqueuePending(Action *action) {
  assert(freeList != NULL);
  ActionListNode *node = freeList;
  freeList = freeList->next;
  
  pendingCount += 1;
  node->next = NULL;  
  if (tail == NULL) {
    assert(head == NULL && pendingCount == 0);
    node->prev = NULL;
    head = node;  
  }
  else {
    assert(head != NULL && pendingCount != 0);
    tail->next = NULL;
    node->prev = tail;
  }
  tail = node;
}

inline void PendingActionList::DequeuePending(ActionListNode *node) {
  
  if (node->next == NULL && node->prev == NULL) {
    head = NULL;
    tail = NULL;
  }
  else if (node->next == NULL) {
    tail = node->prev;    
  }
  else if (node->prev == NULL) {
    head = node->next;
  }
  else {
    node->prev->next = node->next;
    node->next->prev = node->prev;
  }
  
  node->prev = NULL;
  node->action = NULL;
  node->next = freeList;
  freeList = node;
}

inline void PendingActionList::ResetCursor() {
  cursor = head;
}

inline ActionListNode* PendingActionList::GetNext() {
  return cursor;
}

inline bool PendingActionList::IsEmpty() {
  return head == NULL;
}

Executor::Executor(ExecutorConfig cfg) : Runnable (cfg.cpu) {
  
}

void Executor::Init() {
  this->allocators = 
    (RecordAllocator**)malloc(sizeof(RecordAllocator*)*config.numTables);
  for (uint32_t i = 0; i < config.numTables; ++i) {
    uint32_t recSize = config.recordSizes[i];
    uint32_t allocSize = config.allocatorSizes[i];
    this->allocators[i] = new RecordAllocator(recSize, allocSize, config.cpu);
  }
}

void Executor::StartWorking() {
  uint32_t epoch = 1;
  while (true) {
    
    // Process the new batch of transactions
    ActionBatch batch = config.inputQueue->DequeueBlocking();    
    ProcessBatch(batch);
    
    // Tell other threads that this epoch is finished
    barrier();
    config.epochPtr[config.threadId] = epoch;
    barrier();

    // If this is the leader thread, try to advance the low-water mark to 
    // trigger garbage collection
    if (config.threadId == 0) {
      uint32_t minEpoch = *config.epochPtr;
      for (uint32_t i = 1; i < config.numExecutors; ++i) {
        barrier();
        uint32_t temp = config.epochPtr[i];
        barrier();
        
        if (temp < minEpoch) {
          minEpoch = temp;
        }
      }
      barrier();
      *config.lowWaterMarkPtr = minEpoch;
      barrier();
    }
    
    // Try to return records that are no longer visible to their owners
    garbageBin->FinishEpoch(epoch);
    
    // Check if there's any garbage we can recycle. Insert into our record 
    // allocators.
    RecycleData();
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
        allocators[i]->Recycle(recycled);
      }
    }
  }
}

void Executor::ProcessBatch(const ActionBatch &batch) {

  for (uint32_t i = config.threadId; i < batch.numActions; 
       i += config.numExecutors) {
    Action *cur = batch.actionBuf[i];
    if (!ProcessSingle(cur)) {
      pendingList->EnqueuePending(cur);
    }
  }
  
  while (!pendingList->IsEmpty()) {
    pendingList->ResetCursor();
    for (ActionListNode *node = pendingList->GetNext(); node != NULL; 
         node = pendingList->GetNext()) {
      if (ProcessSingle(node->action)) {
        pendingList->DequeuePending(node);
      }
    }
  }
}

bool Executor::ProcessSingle(Action *action) {
  assert(action != NULL);
  if (action->state != SUBSTANTIATED) {
    if (cmp_and_swap(&action->state, STICKY, PROCESSING)) {
      if (ProcessTxn(action)) {
        return true;
      }
      else {
        action->state = STICKY;
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
  uint32_t numReads = action->readset.size();
  uint32_t numWrites = action->writeset.size();

  // First ensure that all transactions on which the current one depends on have
  // been processed.
  for (size_t i = 0; i < numReads; ++i) {
    CompositeKey curKey = action->readset[i];
    MVRecord *record = 
      DB.GetTable(curKey.tableId)->GetMVRecord(curKey.tableId, curKey, 
                                               action->version);
    Action *dependAction = record->writer;    

    if (dependAction == NULL || ProcessSingle(dependAction)) {
      // Track the value of the record.
      action->readset[i].value = record;
    }
    else {
      // Record not available, another thread is processing dependAction.
      ready = false;
    }
  }

  for (size_t i = 0; i < numWrites; ++i) {
    CompositeKey curKey = action->writeset[i];
    MVRecord *record = 
      DB.GetTable(curKey.tableId)->GetMVRecord(curKey.tableId, curKey, 
                                               action->version);
    Action *dependAction = record->writer;
    
    if (dependAction == NULL || ProcessSingle(dependAction)) {
      action->writeset[i].value = record;
    }
    else {
      ready = false;
    }
  }
  
  if (ready) {
    if (action->Run()) {
      // Install updates      
      uint32_t numWrites = action->writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        MVRecord *previous = action->writeset[i].value->recordLink;
        if (previous != NULL && previous->value != NULL) {
          garbageBin->AddRecord(previous->writingThread, 
                                action->writeset[i].tableId,
                                previous->value);
        }
      }
    }
    else {
      // Abort. 
      uint32_t numWrites = action->writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        MVRecord *previous = action->writeset[i].value->recordLink;
        if (previous != NULL) {
          action->writeset[i].value->value = previous->value;
        }
      }
    }

    // Add MVRecords and Records rendered invisible by this update
    for (uint32_t i = 0; i < action->writeset.size(); ++i) {
      MVRecord *deleted = action->writeset[i].value->recordLink;
      if (deleted != NULL) {
        garbageBin->AddMVRecord(action->writeset[i].threadId, deleted);
      }
    }
  }  
  return ready;
}

void GarbageBin::AddMVRecord(uint32_t ccThread, MVRecord *rec) {
  rec->allocLink = NULL;
  *(curStickies[ccThread].tail) = rec;
  curStickies[ccThread].tail = &rec->allocLink;
}

void GarbageBin::AddRecord(uint32_t workerThread, uint32_t tableId, 
                           Record *rec) {
  rec->next = NULL;
  *(curRecords[workerThread][tableId].tail) = rec;
  curRecords[workerThread][tableId].tail = &rec->next;  
}

void GarbageBin::ReturnGarbage() {
  for (uint32_t i = 0; i < config.numCCThreads; ++i) {
    
    // Try to enqueue garbage. If enqueue fails, we'll just try again during the
    // next call.
    if (!config.ccChannels[i]->Enqueue(snapshotStickies[i])) {
      *(curStickies[i].tail) = snapshotStickies[i].head;
      curStickies[i].tail = snapshotStickies[i].tail;
    }
    snapshotStickies[i] = curStickies[i];
    curStickies[i].head = NULL;
    curStickies[i].tail = &curStickies[i].head;
  }
  
  uint32_t tblCount = config.numTables;
  for (uint32_t i = 0; i < config.numWorkers; ++i) {
    for (uint32_t j = 0; j < tblCount; ++j) {
      
      // Same logic as "stickies"
      if (!config.workerChannels[i*tblCount+j]->
          Enqueue(snapshotRecords[i][j])) {
        *(curRecords[i][j].tail) = snapshotRecords[i][j].head;
        curRecords[i][j].tail = snapshotRecords[i][j].tail;
      }
      snapshotRecords[i][j] = curRecords[i][j];
      curRecords[i][j].head = NULL;
      curRecords[i][j].tail = &curRecords[i][j].head;
    }
  }
}

void GarbageBin::FinishEpoch(uint32_t epoch) {
  barrier();
  uint32_t lowWatermark = *config.lowWaterMarkPtr;
  barrier();
  
  if (lowWatermark >= snapshotEpoch) {
    ReturnGarbage();
    snapshotEpoch = epoch;
  }
}

RecordAllocator::RecordAllocator(size_t recordSize, uint32_t numRecords, 
                                 int cpu) {
  Record *data = 
    (Record*)alloc_mem(numRecords*(sizeof(Record)+recordSize), cpu);
  memset(data, 0x00, numRecords*(sizeof(Record)+recordSize));
  for (uint32_t i = 0; i < numRecords; ++i) {
    data[i].next = &data[i+1];
  }
  data[numRecords-1].next = NULL;
  freeList = data;
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
