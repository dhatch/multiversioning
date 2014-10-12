#include <lock_manager.h>
#include <cpuinfo.h>
#include <util.h>

LockBucket::LockBucket() {
  this->tail = NULL;
  this->head = NULL;
  this->lockWord = 0;
}

void LockBucket::AppendEntry(LockBucketEntry *entry, uint32_t threadId) {
  entry->next = NULL;
  //  LockBucketEntry *oldTail;
  reentrant_lock(&this->lockWord, threadId);
  if (tail != NULL) {
      tail->next = entry;
  }
  else {
      head = entry;
  }
  tail = entry;
  /*
  oldTail = tail;
  tail = entry;
  if (oldTail == NULL) {
    this->head = entry;
  }
  else {
    oldTail->next = entry;
  }
  */
}

void LockBucket::ReleaseLock() {
  //    unlock(&this->lockWord);
    reentrant_unlock(&this->lockWord);
}

LockManagerTable::LockManagerTable(uint64_t numEntries) {
  this->numEntries = numEntries;
  void *buckets = malloc(sizeof(LockBucket)*numEntries);
  assert(buckets != NULL);
  memset(buckets, 0x00, sizeof(LockBucket)*numEntries);
  this->entries = (LockBucket*)buckets;
}

inline void LockManagerTable::AcquireLock(LockingCompositeKey *key, 
                                          uint32_t threadId) {
  uint64_t bucketNumber = LockingCompositeKey::Hash(key) % this->numEntries;
  this->entries[bucketNumber].AppendEntry(&key->bucketEntry,
                                          threadId);
}

inline void LockManagerTable::CompleteLockPhase(LockingCompositeKey *key) {
  uint64_t bucketNumber = 
      LockingCompositeKey::Hash(key) % this->numEntries;
    this->entries[bucketNumber].ReleaseLock();
}

LockManager::LockManager(const unordered_map<uint32_t, uint64_t>& tableInfo, 
                         uint32_t numTables) {
  this->numTables = numTables;
  for (auto iter = tableInfo.begin(); iter != tableInfo.end(); ++iter) {
    uint32_t tableId = iter->first;
    uint64_t tableSize = iter->second;
    LockManagerTable *tbl = new LockManagerTable(tableSize);    
    this->tables[tableId] = tbl;
  }
}

void LockManager::AcquireLocks(LockingAction *action, uint32_t threadId) {
  int writesetSize = action->writeset.size();
  for (int i = 0; i < writesetSize; ++i) {
    uint32_t tbl = action->writeset[i].tableId;
    assert(tbl < this->numTables);
    tables[tbl]->AcquireLock(&action->writeset[i], threadId);
  }
  
  barrier();

  for (int i = 0; i < writesetSize; ++i) {
    uint32_t tbl = action->writeset[i].tableId;
    tables[tbl]->CompleteLockPhase(&action->writeset[i]);
  }
}

BucketEntryAllocator::BucketEntryAllocator(uint32_t numEntries, int cpu) {
  // Allocate lock bucket entries. Link them together.
  LockBucketEntry *entries = 
    (LockBucketEntry*)alloc_mem(((size_t)numEntries)*sizeof(LockBucketEntry), 
                                cpu);
  assert(entries != NULL);
  memset(entries, 0x0, ((size_t)numEntries)*sizeof(LockBucketEntry));
  
  // Avoid locality from sequential allocation.
  for (uint32_t i = 0; i < numEntries/2; ++i) {
    entries[i].next = &entries[numEntries-1-i];
    entries[numEntries-1-i].next = &entries[i+1];
  }
  entries[numEntries/2].next = NULL;
  freeList = entries;
}

bool BucketEntryAllocator::GetEntry(LockBucketEntry **OUT_ENTRY) {
  if (freeList != NULL) {
    LockBucketEntry *ret = freeList;
    freeList = (LockBucketEntry*)freeList->next;
    ret->next = NULL;
    ret->action = NULL;
    *OUT_ENTRY = ret;
    return true;
  }
  else {
    *OUT_ENTRY = NULL;
    return false;
  }
}

void BucketEntryAllocator::ReturnEntry(LockBucketEntry *entry) {
  entry->action = NULL;
  entry->next = freeList;
  freeList = entry;
}
