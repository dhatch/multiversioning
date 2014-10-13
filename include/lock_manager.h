#ifndef         LOCK_MANAGER_H_
#define         LOCK_MANAGER_H_

#include <action.h>
#include <cpuinfo.h>
#include <unordered_map>

using namespace std;

class LockBucket {  
 public:
    volatile LockBucketEntry *tail;
   volatile LockBucketEntry *head;
  volatile uint64_t lockWord;
  LockBucket();
  
  void AppendEntry(LockBucketEntry *entry, uint32_t threadId);
  void ReleaseLock();
} __attribute__((__packed__, __aligned__(CACHE_LINE)));

class LockManagerTable {
 private:
  LockBucket *entries;
  uint64_t numEntries;

 public:
  LockManagerTable(uint64_t numEntries, uint32_t threads);
  
  void AcquireLock(LockingCompositeKey *key, uint32_t threadId);
  
  void CompleteLockPhase(LockingCompositeKey *key);  
};

class BucketEntryAllocator {
 private:
  LockBucketEntry *freeList;

 public:
  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }

  BucketEntryAllocator(uint32_t numEntries, int cpu);
  
  bool GetEntry(LockBucketEntry **OUT_ENTRY);
  
  void ReturnEntry(LockBucketEntry *entry);
};

class LockManager {
  friend class LockManagerTest;

 private:
  LockManagerTable **tables;
  uint32_t numTables;
  
 public:
  LockManager(const unordered_map<uint32_t, uint64_t>& tableInfo, uint32_t numTables, uint32_t threads);

  void AcquireLocks(LockingAction *action, uint32_t threadId);
};

#endif          // LOCK_MANAGER_H_
