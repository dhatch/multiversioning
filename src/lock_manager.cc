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

LockManager::LockManager(uint64_t numEntries, int cpu) {
  this->numEntries = numEntries;
  void *lockManagerBuckets = alloc_mem(sizeof(LockBucket)*numEntries, cpu);
  assert(lockManagerBuckets != NULL);
  memset(lockManagerBuckets, 0x0, sizeof(LockBucket)*numEntries);
  this->entries = (LockBucket*)lockManagerBuckets;
  
}

void LockManager::AcquireLocks(LockingAction *action, uint32_t threadId) {
  int writesetSize = action->writeset.size();
  for (int i = 0; i < writesetSize; ++i) {
    uint64_t bucketNumber = 
      LockingCompositeKey::Hash(&action->writeset[i]) % this->numEntries;
    this->entries[bucketNumber].AppendEntry(&action->writeset[i].bucketEntry, 
                                            threadId);
  }
  
  barrier();

  for (int i = 0; i < writesetSize; ++i) {
    uint64_t bucketNumber = 
      LockingCompositeKey::Hash(&action->writeset[i]) % this->numEntries;
    this->entries[bucketNumber].ReleaseLock();
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
