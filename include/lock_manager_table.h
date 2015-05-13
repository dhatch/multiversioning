#ifndef         LOCK_MANAGER_TABLE_H_
#define         LOCK_MANAGER_TABLE_H_

#include <cpuinfo.h>
#include <util.h>
#include <action.h>
#include <machine.h>
#include <lock_manager.h>

struct LockBucket {
  EagerRecordInfo *head;
  EagerRecordInfo *tail;
  volatile uint64_t latch;
} __attribute__((__packed__, __aligned__(CACHE_LINE)));


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

  void WakeupReaders(EagerRecordInfo *iter, const EagerCompositeKey &key) {

    assert(iter != NULL);
    assert(iter->record == key);
    assert(!iter->is_write);
    
    while (iter != NULL && iter->record == key && !iter->is_write) {
      fetch_and_decrement(&iter->dependency->num_dependencies);
      iter = iter->next;
    }
  }

  void ReleaseLock(EagerRecordInfo *iter, const EagerCompositeKey &key) {
    while (iter != NULL && iter->record != key) {
      if (iter->record == key) {
        if (iter->is_write) {         
          fetch_and_decrement(&iter->dependency->num_dependencies);
        }
        else {
          WakeupReaders(iter, key);
        }
        return;
      }
    }
  }

  bool Conflicting(EagerRecordInfo *info1, EagerRecordInfo *info2) {
    return ((info1->record == info2->record) && 
            (info1->is_write || info2->is_write));
  }

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

  LockBucket* GetBucketRef(const EagerCompositeKey &key) {
    // Get the table
    char *tbl = tables[key.tableId];
    uint64_t tblSz = tableSizes[key.tableId];

    // Get the bucket in the table
    uint64_t index = key.Hash() % tblSz;
    char *bucketPtr = &tbl[CACHE_LINE*index];
    return (LockBucket*)bucketPtr;
  }


  bool GetNext(EagerRecordInfo *info, EagerRecordInfo **next) {
    EagerRecordInfo *cur = info->next;
    while (cur != NULL) {
      if (cur->record == info->record) {
        *next = cur;
        return true;
      }
      cur = cur->next;
    }
    *next = NULL;
    return false;
  }

  void AdjustWrite(EagerRecordInfo *info) {
    assert(info->is_write);
    EagerRecordInfo *descendant;
    
    if (GetNext(info, &descendant)) {
      if (descendant->is_write) {
        descendant->is_held = true;
        fetch_and_decrement(&descendant->dependency->num_dependencies);
      }
      else {
        EagerRecordInfo *temp = descendant;
        do {
          descendant = temp;
          descendant->is_held = true;
          fetch_and_decrement(&descendant->dependency->num_dependencies);
        } while (GetNext(descendant, &temp) && !temp->is_write);
      }
    }  
  }

  void AdjustRead(EagerRecordInfo *info, LockBucket *bucket) {

    assert(!info->is_write);
    EagerRecordInfo *descendant;
    
    EagerRecordInfo *prev = bucket->head;    
    assert(prev != NULL);
    while (prev->record != info->record) {
      prev = prev->next;
      assert(prev != NULL);
    }

    if ((prev == info) && GetNext(info, &descendant)) {
      if (descendant->is_write) {
        descendant->is_held = true;
        fetch_and_decrement(&descendant->dependency->num_dependencies);        
      }
    }
  }

  EagerRecordInfo* SearchRead(LockBucket *bucket, EagerRecordInfo *info) {
          assert(bucket->head != NULL && bucket->tail != NULL);
          assert(info->is_write == false);
          
          EagerRecordInfo *iter;
          iter = bucket->head;
          while (iter != NULL) {
                  if (iter->is_write == false &&
                      iter->record.tableId == info->record.tableId &&
                      iter->record.key == info->record.key)
                          return iter;
                  else
                          iter = iter->next;
          }
          return NULL;
  }
  
  void AppendInfo(EagerRecordInfo *info, LockBucket *bucket) {
    assert((bucket->head == NULL && bucket->tail == NULL) || 
           (bucket->head != NULL && bucket->tail != NULL));

    EagerRecordInfo *read_next;
    
    info->next = NULL;
    info->prev = NULL;

    if (bucket->tail == NULL) {
      assert(bucket->head == NULL);
      info->prev = NULL;
      bucket->head = info;
      bucket->tail = info;
    } else {
      assert(bucket->head != NULL);
      if (info->is_write == false &&
          (read_next = SearchRead(bucket, info)) != NULL &&
          bucket->tail != read_next) {
              assert(read_next->is_write == false);
              read_next->next->prev = info;
              info->next = read_next->next;
              info->prev = read_next;
              read_next->next = info;
      } else {
              bucket->tail->next = info;
              info->prev = bucket->tail;
              bucket->tail = info;
      }
    }

    //    bucket->tail = info;
    assert(bucket->head != NULL && bucket->tail != NULL);
  }
  
  void RemoveInfo(EagerRecordInfo *info, LockBucket *bucket) {
    assert((bucket->head != NULL && bucket->tail != NULL));

    if (info->next == NULL) {
      bucket->tail = info->prev;
    }
    else {
      info->next->prev = info->prev;
    }
  
    if (info->prev == NULL) {
      bucket->head = info->next;
    }
    else {
      info->prev->next = info->next;
    }
    info->prev = NULL;
    info->next = NULL;

    assert((bucket->head == NULL && bucket->tail == NULL) || 
           (bucket->head != NULL && bucket->tail != NULL));
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
  

  bool Lock(EagerRecordInfo *info, uint32_t cpu __attribute__((unused))) {
    assert(cpu > 0);
    bool conflict = false;
    LockBucket *bucket = GetBucketRef(info->record);
        
    //    reentrant_lock(&bucket->latch, cpu);
    lock(&bucket->latch);
    //    assert(bucket->latch>>32 == cpu);
    info->latch = &bucket->latch;    

    AppendInfo(info, bucket);

    EagerRecordInfo *cur = info->prev;
    while (cur != NULL) {
      if ((conflict = Conflicting(cur, info)) == true) {
        fetch_and_increment(&info->dependency->num_dependencies);
        break;
      }
      else if (cur->record == info->record && cur->is_held) {
        break;
      }
      cur = cur->prev;
    }
    
    info->is_held = !conflict;
    //    reentrant_unlock(&bucket->latch);
    unlock(&bucket->latch);
    return !conflict;
  }
  
  void FinishLock(EagerRecordInfo *info) {
    reentrant_unlock(info->latch);
  }

  void Unlock(EagerRecordInfo *info, uint32_t cpu __attribute__((unused))) {

    assert(info->is_held);

    LockBucket *bucket = GetBucketRef(info->record);    
    //    reentrant_lock(&bucket->latch, cpu);
    lock(&bucket->latch);
    //    assert(bucket->latch>>32 == cpu);

    if (info->is_write) {
      AdjustWrite(info);
    }
    else {
      AdjustRead(info, bucket);
    }

    RemoveInfo(info, bucket);
    unlock(&bucket->latch);
    //    reentrant_unlock(&bucket->latch);
  }

  // Get a pointer to the head of linked list of records.
  /*
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
  */

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
