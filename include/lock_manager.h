#ifndef 	LOCK_MANAGER_H_
#define		LOCK_MANAGER_H_

#include <action.h>

class LockBucket {  
 public:
  volatile LockBucketEntry *tail;
  volatile LockBucketEntry *head;
  
  LockBucket();
  
  void AppendEntry(LockBucketEntry *entry);
} __attribute__((__packed__, __aligned__(CACHE_LINE)));

class LockManager {
  friend class LockManagerTest;

 private:
  LockBucket *entries;
  uint64_t numEntries;
  
 public:
  LockManager(uint64_t numEntries, int cpu);

  void AcquireLocks(LockingAction *action);
};

#endif 		// LOCK_MANAGER_H_
