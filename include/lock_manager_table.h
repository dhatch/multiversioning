#ifndef         LOCK_MANAGER_TABLE_H_
#define         LOCK_MANAGER_TABLE_H_

#include <cpuinfo.h>
#include <util.h>
#include <action.h>
#include <machine.h>
#include <lock_manager.h>


struct TxnQueue {
  EagerRecordInfo *head;
  EagerRecordInfo *tail;
  volatile uint64_t __attribute((aligned(CACHE_LINE))) lock_word;

  EagerCompositeKey key;

  TxnQueue *next;
  //  TxnQueue *prev;

  TxnQueue() {
    head = NULL;
    tail = NULL;
    lock_word = 0;
  }
};

class TxnQueueAllocator {
 private:
  TxnQueue *freeList;

 public:
  void* operator new(std::size_t sz, int cpu) {
    return alloc_mem(sz, cpu);
  }

  TxnQueueAllocator(uint64_t numEntries, int cpu) {
    freeList = (TxnQueue*)alloc_mem(sizeof(TxnQueue)*numEntries, cpu);
    memset(freeList, 0x00, sizeof(TxnQueue)*numEntries);
    
    for (uint64_t i = 0; i < numEntries; ++i) {
      freeList[i].next = &freeList[i+1];
    }
    freeList[numEntries-1].next = NULL;
  }

  TxnQueue *Get() {
    assert(freeList != NULL);
    TxnQueue *ret = freeList;
    freeList = freeList->next;
    ret->next = NULL;
    ret->head = NULL;
    ret->tail = NULL;
    ret->lock_word = 0;
    return ret;
  }
};

struct LockManagerConfig {
  uint32_t numTables;
  uint64_t *tableSizes;
  int startCpu;
  int endCpu;
  uint64_t allocatorSize;
};

class LockManagerTable {

 private:
  char **tables;
  TxnQueueAllocator **allocators;
  uint64_t *tableSizes;
  
  int startCpu;
  int endCpu;

  static const uint64_t BUCKET_SIZE = CACHE_LINE;

  // Try to insert "toPut" into the bucket. If there already exists another 
  // record of the same key, return a reference to it.
  /*
  bool TryInsert(EagerRecordInfo **iter, EagerRecordInfo *toPut, 
                 EagerRecordInfo **OUT) {
    while (*iter != NULL) {
      if ((*iter)->record == toPut->record) {
        // There already exists a record
        *OUT = *iter;
        return false;
      }
      else {
        iter = &((*iter)->next);
      }
    }    
    
    if (*iter == NULL) {
      // Obtain a reference to the record containing iter
      uint32_t offset = offsetof(EagerRecordInfo, next);
      EagerRecordInfo *iterHead = (EagerRecordInfo*)(((char*)(iter)) - offset);
      assert(&iterHead->next == iter);

      // Insert
      *iter = toPut;
      toPut->prev = iterHead;
      toPut->next = NULL;
      
      *OUT = toPut;
      return true;
    }
    assert(false);
  }
  */


  // Find the record in the appropriate hash bucket. Assumes that the latch is 
  // set.
  TxnQueue* FindRecord(TxnQueue **iter, const EagerCompositeKey &key, int cpu) {
    // Check if there is a pre-existing record
    while (*iter != NULL) {
      if ((*iter)->key == key) {
        return *iter;
      }
      else {
        iter = &((*iter)->next);
      }
    }
    
    // Couldn't find anything, create a new record
    TxnQueue *toAdd = allocators[cpu]->Get();
    toAdd->key = key;
    toAdd->next = NULL;
    *iter = toAdd;
    return toAdd;
  }

  void GetBucketRef(const EagerCompositeKey &key, TxnQueue ***OUT_DATA, 
                    char **OUT_LATCH) {
    // Get the table
    char *tbl = tables[key.tableId];
    uint64_t tblSz = tableSizes[key.tableId];

    // Get the bucket in the table
    uint64_t index = key.Hash() % tblSz;
    char *bucketPtr = &tbl[CACHE_LINE*index];
    *OUT_LATCH = bucketPtr;
    *OUT_DATA = (TxnQueue**)(bucketPtr+sizeof(uint64_t));
  }

 public:
  LockManagerTable(LockManagerConfig config) {
    this->startCpu = config.startCpu;
    this->endCpu = config.endCpu;
    this->tableSizes = config.tableSizes;

    uint64_t totalSz = 0;
    for (uint32_t i = 0; i < config.numTables; ++i) {      
      totalSz += config.tableSizes[i]*CACHE_LINE;
    }
    
    // Allocate data for lock manager hash table
    char *data = (char*)alloc_interleaved(totalSz, config.startCpu, 
                                          config.endCpu);
    memset(data, 0x0, totalSz);
    
    this->tables = (char**)alloc_mem(config.numTables*sizeof(char*), 
                                     config.startCpu);
    memset(this->tables, 0x0, config.numTables*sizeof(char*));
    this->tableSizes = tableSizes;
    
    // Setup pointers to hash tables appropriately
    uint64_t prevSize = 0;
    for (uint32_t i = 0; i < config.numTables; ++i) {
      this->tables[i] = &data[prevSize];
      prevSize += tableSizes[i]*CACHE_LINE;
    }
    
    // Setup struct TxnQueue allocators
    this->allocators = 
      (TxnQueueAllocator**)malloc(sizeof(TxnQueueAllocator)*
                                  (config.endCpu-config.startCpu+1));
    for (int i = 0; i < config.endCpu-config.startCpu+1; ++i) {
      this->allocators[i] = new (i) TxnQueueAllocator(config.allocatorSize, i);
    }
  }
  
  // Get a pointer to the head of linked list of records.
  TxnQueue* GetPtr(const EagerCompositeKey &key, int cpu) {
    assert(cpu >= startCpu && cpu <= endCpu);
    char *latch;
    TxnQueue **dataPtr = NULL;
    GetBucketRef(key, &dataPtr, &latch);

    // latch the bucket
    lock((volatile uint64_t*)latch);
    
    TxnQueue *ret = FindRecord(dataPtr, key, cpu);    

    // unlatch the bucket
    unlock((volatile uint64_t*)latch);
    assert(ret != NULL);
    return ret;
  }

  /*
  bool GetPtr(const EagerCompositeKey &key, EagerRecordInfo **OUT) {
    char *latch;
    EagerRecordInfo **dataPtr = NULL;
    GetBucketRef(key, &dataPtr, &latch);

    // latch the bucket
    lock((volatile uint64_t*)latch);

    // find the appropriate record
    bool ret = FindRecord(dataPtr, key, OUT);

    // unlatch the bucket
    unlock((volatile uint64_t*)latch);
    return ret;
  }
  */

  /*
  void Remove(EagerRecordInfo *info) {
    char *latch;
    EagerRecordInfo **dataPtr;
    GetBucketRef(info->record, &dataPtr, &latch);
    
    lock(latch);

    EagerRecordInfo *lst;
    bool success = FindRecord(dataPtr, &lst);
    assert(success);
    
    unlock(latch);
  }
  */
};

#endif          // LOCK_MANAGER_TABLE_H_
