#include <lock_manager.h>
#include <cpuinfo.h>
#include <util.h>

LockBucket::LockBucket() {
  this->tail = NULL;
  this->head = NULL;
}

void LockBucket::AppendEntry(LockBucketEntry *entry) {
  entry->next = NULL;
  LockBucketEntry *oldTail = (LockBucketEntry*)xchgq((uint64_t*)&tail, 
                                                     (uint64_t)entry);
  if (oldTail == NULL) {
    this->head = entry;
  }
  else {
    oldTail->next = entry;
  }
}

LockManager::LockManager(uint64_t numEntries, int cpu) {
  this->numEntries = numEntries;
  void *lockManagerBuckets = alloc_mem(sizeof(LockBucket)*numEntries, cpu);
  assert(lockManagerBuckets != NULL);
  memset(lockManagerBuckets, 0x0, sizeof(LockBucket)*numEntries);
  this->entries = (LockBucket*)lockManagerBuckets;
  
}

void LockManager::AcquireLocks(LockingAction *action) {
  int writesetSize = action->writeset.size();
  for (int i = 0; i < writesetSize; ++i) {
    uint64_t bucketNumber = LockingCompositeKey::Hash(&action->writeset[i]);
    this->entries[bucketNumber].AppendEntry(&action->writeset[i].bucketEntry);
  }
}
