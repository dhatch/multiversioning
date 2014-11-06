#ifndef ACTION_H
#define ACTION_H

#include <cassert>
#include <vector>
#include <stdint.h>
#include "machine.h"
#include <pthread.h>
#include <time.h>
#include <cstring>
#include <city.h>
#include <mv_record.h>
#include <util.h>

class Action;

extern uint64_t recordSize;

enum ActionState {
  STICKY,
  PROCESSING,
  SUBSTANTIATED,
};

struct Record {
  Record *next;
  char value[0];
};

struct RecordList {
  Record *head;
  Record **tail;
  uint64_t count;
};

class CompositeKey {
 public:
  uint32_t tableId;
  uint64_t key;
  uint32_t threadId;
  MVRecord *value;
  
  CompositeKey(uint32_t table, uint64_t key) {
    this->tableId = table;
    this->key = key;
    this->value = NULL;
  }
  
  CompositeKey() {
    this->tableId = 0;
    this->key = 0;
    this->value = NULL;
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

  void* Read(uint32_t index) {
    return (void*)(&readset[index].value->value->value);
  }

  void* GetWriteRef(uint32_t index) {
    
    // Memory for the write should always be initialized.
    assert(writeset[index].value->value != NULL);
    return (void*)(&writeset[index].value->value->value);
  }

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

  //  char readVersions[64*10];
  //  VersionBuffer readVersions[10];

  //  std::vector<int> real_writes;
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_start_time;
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) sched_end_time;  
  //  volatile uint64_t __attribute__((aligned(CACHE_LINE))) lock_word;

  volatile uint64_t __attribute__((aligned(CACHE_LINE))) state;
  virtual bool Run() { 
    /*
    for (int i = 0; i < 10000; ++i) {
      single_work();
    }
    */
    return true; 
  }
  //  virtual bool IsLinked(Action **cont) { *cont = NULL; return false; }
};

// Use this action to populate the database
class InsertAction : public Action {
 public:
  virtual bool Run() {
    if (recordSize == 8) {
      uint32_t numWrites = writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t key = writeset[i].key;
        uint64_t *ref = (uint64_t*)GetWriteRef(i);
        *ref = key;
      }
    }
    else if (recordSize == 1000) {
      uint32_t numWrites = writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *ref = (uint64_t*)GetWriteRef(i);
        for (uint32_t j = 0; j < 125; ++j) {
          ref[j] = (uint64_t)rand();
        }
      }
    }
    else {
      assert(false);
    }
    return true;
  }
};

// Use this action to evaluate RMW workload on integers
class RMWAction : public Action {
 public:
  virtual bool Run() {
    if (recordSize == 8) {
      // Accumulate all read values into counter
      uint64_t counter = 0;
      uint32_t numReads = readset.size();
      for (uint32_t i = 0; i < numReads; ++i) {
        uint64_t *readRef = (uint64_t*)Read(i);
        counter += *readRef;
      }
    
      // Add counter to each record in write set
      uint32_t numWrites = writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *writeRef = (uint64_t*)GetWriteRef(i);
        *writeRef += counter;
      }
    }
    else if (recordSize == 1000) {
      // Accumulate all read values into counter
      uint64_t counter = 0;
      uint32_t numReads = readset.size();
      for (uint32_t i = 0; i < numReads; ++i) {
        uint64_t *readRef = (uint64_t*)Read(i);
        for (uint32_t j = 0; j < 125; ++j) {
          counter += readRef[j];
        }
      }
    
      // Add counter to each record in write set
      uint32_t numWrites = writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *writeRef = (uint64_t*)GetWriteRef(i);
        for (uint32_t j = 0; j < 125; ++j) {
          if (j % 8 == 0) {
            counter *= 2;
          }
          writeRef[j] = counter;
        }
      }
    }

    else {
      assert(false);
    }
    return true;
  }
};

class EagerCompositeKey {
 public:
  uint32_t tableId;
  uint64_t key;

  EagerCompositeKey(uint32_t table, uint64_t key) {
    this->tableId = table;
    this->key = key;
  }
  
  EagerCompositeKey() {
    this->tableId = 0;
    this->key = 0;
  }

  bool operator==(const EagerCompositeKey &other) const {
    return other.tableId == this->tableId && other.key == this->key;
  }

  bool operator!=(const EagerCompositeKey &other) const {
    return !(*this == other);
  }
  
  bool operator<(const EagerCompositeKey &other) const {
      return ((this->tableId < other.tableId) || 
              ((this->tableId == other.tableId) && (this->key < other.key)));
  }
  
  bool operator>(const EagerCompositeKey &other) const {
      return ((this->tableId > other.tableId) ||
              ((this->tableId == other.tableId) && (this->key > other.key)));
  }
  
  bool operator<=(const EagerCompositeKey &other) const {
      return !(*this > other);
  }
  
  bool operator>=(const EagerCompositeKey &other) const {
      return !(*this < other);
  }

  inline uint64_t Hash() const {
    return Hash128to64(std::make_pair(key, (uint64_t)(tableId)));
  }
};

class EagerAction;
struct EagerRecordInfo {
  EagerCompositeKey record;
  EagerAction  *dependency;
  bool is_write;
  bool is_held;
  volatile uint64_t *latch;
  struct EagerRecordInfo *next;
  struct EagerRecordInfo *prev;
  void *value;
    
  EagerRecordInfo() {
    record.tableId = 0;
    record.key = 0;
    dependency = NULL;
    is_write = false;
    is_held = false;
    next = NULL;
    prev = NULL;
  }

  bool operator<(const struct EagerRecordInfo &other) const {
    return (this->record < other.record);
  }
    
  bool operator>(const struct EagerRecordInfo &other) const {
    return (this->record > other.record);
  }
    
  bool operator==(const struct EagerRecordInfo &other) const {
    return (this->record == other.record);
  }
    
  bool operator!=(const struct EagerRecordInfo &other) const {
    return (this->record != other.record);
  }

  bool operator>=(const struct EagerRecordInfo &other) const {
    return !(this->record < other.record);
  }

  bool operator<=(const struct EagerRecordInfo &other) const {
    return !(this->record > other.record);
  }    

  inline uint64_t Hash() const {
    return this->record.Hash();
  }

};

class EagerAction {
 public:
    volatile uint64_t __attribute__((aligned(CACHE_LINE))) num_dependencies;
    std::vector<struct EagerRecordInfo> writeset;
    std::vector<struct EagerRecordInfo> readset;

    //    timespec start_time;
    //    timespec end_time;

    //    uint64_t start_rdtsc_time;
    //    uint64_t end_rdtsc_time;

    //    virtual bool IsRoot() { return false; }
    //    virtual bool IsLinked(EagerAction **ret) { *ret = NULL; return false; };
    virtual bool Run() { 
      //      for (int i = 0; i < 10000; ++i) {
      //        single_work();
      //      }
      return true; 
    }

    //    virtual void PostExec() { };

    EagerAction *next;
    EagerAction *prev;
    bool        finished_execution;
};

class RMWEagerAction : public EagerAction {
 public:
  virtual bool Run() {
    if (recordSize == 8) {      // longs
      uint64_t counter = 0;
      uint32_t numReads = readset.size();
      for (uint32_t i = 0; i < numReads; ++i) {
        uint64_t *record = (uint64_t*)readset[i].value;
        counter += *record;
      }

      uint32_t numWrites = writeset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *record = (uint64_t*)writeset[i].value;
        counter += *record;
      }

      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *record = (uint64_t*)writeset[i].value;
        *record += counter;
      }
    }
    else if (recordSize == 1000) {      //YCSB
      uint32_t numReads = readset.size();
      uint32_t numWrites = writeset.size();
      
      uint64_t counter = 0;
        
      // Read the ith field
      for (uint32_t j = 0; j < numReads; ++j) {
        uint64_t *record = (uint64_t*)readset[j].value;
        for (uint32_t i = 0; i < 125; ++i) {
          counter += record[i];
        }
      }
      for (uint32_t j = 0; j < numWrites; ++j) {
        uint64_t *record = (uint64_t*)writeset[j].value;
        for (uint32_t i = 0; i < 125; ++i) {
          counter += record[i];
        }
      }
        
      // Write the ith field
      for (uint32_t j = 0; j < numWrites; ++j) {
        uint64_t *record = (uint64_t*)writeset[j].value;
        for (uint32_t i = 0; i < 125; ++i) {
          if (i % 8 == 0) {
            counter = counter*2;
          }
          record[i] = counter;
        }
      }
    }

    return true;
  }
};


#endif // ACTION_H
