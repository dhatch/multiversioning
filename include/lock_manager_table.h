#ifndef         LOCK_MANAGER_TABLE_H_
#define         LOCK_MANAGER_TABLE_H_

#include <cpuinfo.h>
#include <util.h>
#include <action.h>
#include <machine.h>
#include <lock_manager.h>
#include <locking_action.h>

struct LockBucket {
        locking_key *head;
        locking_key *tail;
        volatile uint64_t latch;
} __attribute__((__packed__, __aligned__(CACHE_LINE)));

/*
struct TxnQueue {
  locking_key *head;
  locking_key *tail;
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
*/

struct LockManagerConfig {
  uint32_t numTables;
  uint32_t *tableSizes;
  int startCpu;
  int endCpu;
};

class LockManagerTable {

 private:
  char **tables;
  //  TxnQueueAllocator **allocators;
  uint64_t *tableSizes;
  
  int startCpu;
  int endCpu;

  static const uint64_t BUCKET_SIZE = CACHE_LINE;

  bool Conflicting(locking_key *key1, locking_key *key2)
  {
          return (*key1 == *key2) && (key1->is_write || key2->is_write);
  }


  LockBucket* GetBucketRef(const locking_key *key)
  {
          // Get the table
          char *tbl = tables[key->table_id];
          uint64_t tblSz = tableSizes[key->table_id];

          // Get the bucket in the table
          uint64_t index = key->Hash() % tblSz;
          char *bucketPtr = &tbl[CACHE_LINE*index];
          return (LockBucket*)bucketPtr;
  }

  /*
   * Find the next locking_key on the same key as k. Returns true if we're able 
   * to find a descendant, otherwise, return false.
   */
  bool GetNext(locking_key *k, locking_key **next)
  {
          locking_key *cur;

          cur = k->next;
          for (cur = k->next; cur != NULL; cur = cur->next)
                  ;
          *next = cur;
          return *next != NULL;
  }

  /*
   * Pass on a lock to k and hence, its associated transaction. 
   * 
   * XXX Could add some logic to enqueue on a worker thread's ready queue. This 
   * will avoid manually checking for txns with zero dependencies in each 
   * worker's logic.
   */
  void pass_lock(locking_key *k)
  {
          locking_action *act;
          
          assert(k->is_held == false);
          k->is_held = true;
          act = k->dependency;
          fetch_and_decrement(&act->num_dependencies);
  }

  /*
   * k released its write-lock, unlock other txns waiting on k.
   */
  void AdjustWrite(locking_key *k)
  {
          assert(k->is_write);
          locking_key *desc, *temp;
          
          if (GetNext(k, &desc)) {
                  if (desc->is_write) {
                          pass_lock(desc);
                  } else {
                          temp = desc;                          
                          do {
                                  desc = temp;
                                  pass_lock(desc);
                          } while (GetNext(desc, &temp) && !temp->is_write);
                  }
          }  
  }

  /*
   * k released its read-lock, unlock other txns waiting on k.
   */
  void AdjustRead(locking_key *k, LockBucket *bucket)
  {
          assert(!k->is_write);
          locking_key *desc, *prev;

          prev = bucket->head;
          while (*prev != *k) {
                  assert(prev != NULL);
                  prev = prev->next;

          }
          if ((prev == k) && GetNext(k, &desc)) {
                  if (desc->is_write) 
                          pass_lock(desc);
                  assert(desc->is_held == true);
          }
  }

  /*
   * Search the lock bucket for the earliest read-lock on the same key as "k".
   */
  locking_key* SearchRead(LockBucket *bucket, locking_key *k)
  {
          assert(bucket->head != NULL && bucket->tail != NULL);
          assert(k->is_write == false);
          
          locking_key *iter;
          iter = bucket->head;
          while (iter != NULL) {
                  if (iter->is_write == false && *k == *iter)
                          break;
                  iter = iter->next;
          }
          return iter;
  }

  /* 
   * Add a lock request to the lock bucket.
   */ 
  void AppendInfo(locking_key *k, LockBucket *bucket)
  {
          locking_key *read_next;

          /* 
           * If the queue is empty, both head and tail must be NULL, otherwise, 
           * both are non-NULL. 
           */
          assert((bucket->head == NULL && bucket->tail == NULL) || 
                 (bucket->head != NULL && bucket->tail != NULL));    
          k->next = NULL;
          k->prev = NULL;
          
          if (bucket->tail == NULL) {
                  
                  /* The queue of locks is empty. */
                  assert(bucket->head == NULL);
                  k->prev = NULL;
                  bucket->head = k;
                  bucket->tail = k;
          } else {
                  assert(bucket->head != NULL);
                  if (k->is_write == false &&
                      (read_next = SearchRead(bucket, k)) != NULL &&
                      bucket->tail != read_next) {
                          
                          /* 
                           * k is a read-lock -- search for the earliest 
                           * read-lock on the same item. 
                           */
                          assert(read_next->is_write == false);
                          read_next->next->prev = k;
                          k->next = read_next->next;
                          k->prev = read_next;
                          read_next->next = k;
                  } else {

                          /* 
                           * Either k is a write-lock, or we couldn't find any 
                           * other read-locks. Add k to the end of the queue. 
                           */
                          bucket->tail->next = k;
                          k->prev = bucket->tail;
                          bucket->tail = k;
                  }
          }

          /* The queue shouldn't be empty. */
          assert(bucket->head != NULL && bucket->tail != NULL);
  }

  /*
   * Remove k from the bucket's lock queue.
   */
  void RemoveInfo(locking_key *k, LockBucket *bucket)
  {
          /* The queue shouldn't be empty! */
          assert((bucket->head != NULL && bucket->tail != NULL));
          
          if (k->next == NULL) /* k's at the tail. */
                  bucket->tail = k->prev;
          else 
                  k->next->prev = k->prev;
          
          if (k->prev == NULL) /* k is at the head. */
                  bucket->head = k->next;    
          else 
                  k->prev->next = k->next;          
          k->prev = NULL;
          k->next = NULL;          
          assert((bucket->head == NULL && bucket->tail == NULL) || 
                 (bucket->head != NULL && bucket->tail != NULL));
  }

 public:
  
  LockManagerTable(LockManagerConfig config)
  {
          this->startCpu = config.startCpu;
          this->endCpu = config.endCpu;
          this->tableSizes =
                  (uint64_t*)malloc(sizeof(uint64_t)*config.numTables);
          for (uint32_t i = 0; i < config.numTables; ++i) 
                  this->tableSizes[i] = (uint64_t)config.tableSizes[i];

          uint64_t totalSz = 0;
          for (uint32_t i = 0; i < config.numTables; ++i) {      
                  totalSz += config.tableSizes[i]*CACHE_LINE;
          }

          /* Allocate data for lock manager hash table */
          char *data = (char*)alloc_interleaved(totalSz, config.startCpu, 
                                                config.endCpu);
          memset(data, 0x0, totalSz);
    
          this->tables = (char**)alloc_mem(config.numTables*sizeof(char*), 
                                           config.startCpu);
          memset(this->tables, 0x0, config.numTables*sizeof(char*));
          this->tableSizes = tableSizes;

          /* Setup pointers to hash tables appropriately. */
          uint64_t prevSize = 0;
          for (uint32_t i = 0; i < config.numTables; ++i) {
                  this->tables[i] = &data[prevSize];
                  prevSize += tableSizes[i]*CACHE_LINE;
          }
  }

  /*
   * Check if there are any preceding requests that conflict with key.
   */
  bool check_conflict(locking_key *key)
  {
          locking_action *action;
          locking_key *ancestor;
          bool held;
          
          action = key->dependency;
          
          /* Find the earliest entry with the same key */
          ancestor = key->prev;
          while (ancestor != NULL && *key != *ancestor)
                  ;
          if (ancestor == NULL)
                  held = true;
          else if (Conflicting(ancestor, key) == true) /* one is a write */
                  held = false;                  
          else if (ancestor->is_held == true) /* both reads, lock held */
                  held = true;                  
          else /* both reads, lock not held */
                  held = false;
          if (held)
                  fetch_and_increment(&action->num_dependencies);
          key->is_held = held;
          return !held;
  }

  /*
   * Try to acquire the logical lock requested by key. Returns true if the lock 
   * is immediately acquired, otherwise, return false.
   */
  bool Lock(locking_key *key)
  {
          bool conflict;
          LockBucket *bucket;

          bucket = GetBucketRef(key);
          lock(&bucket->latch);
          AppendInfo(key, bucket);
          conflict = check_conflict(key);
          unlock(&bucket->latch);
          return !conflict;
  }

  /*
   * Release the logical lock held by k.
   */
  void Unlock(locking_key *k)
  {
          assert(k->is_held);
          LockBucket *bucket = GetBucketRef(k);    
          lock(&bucket->latch);
          if (k->is_write) 
                  AdjustWrite(k);
          else 
                  AdjustRead(k, bucket);
          RemoveInfo(k, bucket);
          unlock(&bucket->latch);
    }
};

#endif          // LOCK_MANAGER_TABLE_H_
