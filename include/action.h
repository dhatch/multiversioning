#ifndef ACTION_H
#define ACTION_H

#include <cassert>
#include <vector>
#include <stdint.h>
#include "machine.h"
#include <pthread.h>
#include <time.h>
#include <cstring>
//#include <preprocessor.h>
#include <city.h>

class Action;


class CompositeKey {
 public:
  uint32_t tableId;
  uint64_t key;
  uint32_t threadId;

  CompositeKey(uint32_t table, uint64_t key) {
    this->tableId = table;
    this->key = key;
  }
  
  CompositeKey() {
    this->tableId = 0;
    this->key = 0;
  }

  bool operator==(const CompositeKey &other) const {
    return other.tableId == this->tableId && other.key == this->key;
  }

  bool operator!=(const CompositeKey &other) const {
    return !(*this == other);
  }
  
  bool operator<(const CompositeKey &other) const {
      return ((this->tableId < other.tableId) || 
              ((this->tableId == other.tableId) && (this->key < other.key)));
  }
  
  bool operator>(const CompositeKey &other) const {
      return ((this->tableId > other.tableId) ||
              ((this->tableId == other.tableId) && (this->key > other.key)));
  }
  
  bool operator<=(const CompositeKey &other) const {
      return !(*this > other);
  }
  
  bool operator>=(const CompositeKey &other) const {
      return !(*this < other);
  }

  static inline uint64_t Hash(const CompositeKey *key) {
      return Hash128to64(std::make_pair(key->key, (uint64_t)(key->tableId)));
  }
  
  static inline uint64_t HashKey(const CompositeKey *key) {
      return Hash128to64(std::make_pair((uint64_t)key->tableId, key->key));
  }

} __attribute__((__packed__));


struct Range {
    int start;
    int end;
};

class VersionBuffer;

/*
 * An action is used to represent a transaction. A transaction extends this 
 * class. We define "Read" and "Write" methods which are used to access the 
 * appropriate records of the database. The transaction code itself is 
 * completely agnostic about the mapping between a primary key and the 
 * appropriate version. 
 */
class Action {

 protected:
	void* Read(CompositeKey key);
	void* Write(CompositeKey key);

 public:  
    uint64_t version;
    uint64_t combinedHash;
    //  bool materialize;
    //  bool is_blind;  
    //  timespec start_time;
    //  timespec end_time;

    //  uint64_t start_rdtsc_time;
    //  uint64_t end_rdtsc_time;

  //  volatile uint64_t start_time;
  //  volatile uint64_t end_time;
  //  volatile uint64_t system_start_time;
  //  volatile uint64_t system_end_time;
  std::vector<CompositeKey> readset;
  std::vector<CompositeKey> writeset;
  std::vector<Range> readRange;
  std::vector<Range> writeRange;

  //  char readVersions[64*10];
  //  VersionBuffer readVersions[10];

  //  std::vector<int> real_writes;
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_start_time;    
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_end_time;    
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) lock_word;

  volatile uint64_t __attribute__((aligned(CACHE_LINE))) state;
  
  virtual bool NowPhase() { return true; }
  virtual void LaterPhase() { }
  virtual bool IsLinked(Action **cont) { *cont = NULL; return false; }
};


class LockingAction;

struct LockBucketEntry {  
  LockingAction *action;
  volatile LockBucketEntry *next;
};

class LockingCompositeKey {
 public:
  uint32_t tableId;
  uint64_t key;
  LockBucketEntry bucketEntry;

  static inline uint64_t Hash(const LockingCompositeKey *key) {
      return Hash128to64(std::make_pair(key->key, (uint64_t)(key->tableId)));
  }
};

class LockingAction {
 public:  
  volatile uint32_t __attribute__((__packed__, __aligned__(CACHE_LINE))) 
    dependencyCount;
  std::vector<LockingCompositeKey> readset;
  std::vector<LockingCompositeKey> writeset;
  
  virtual bool Execute() { return true; }
};

#endif // ACTION_H
