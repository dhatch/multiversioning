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

#define YCSB_RECORD_SIZE 1000

extern uint32_t NUM_CC_THREADS;
extern uint64_t recordSize;

struct ycsb_record {
        char value[1000];
};


/*
struct Record {
  Record *next;
  char value[0];
};

struct RecordList {
  Record *head;
  Record **tail;
  uint64_t count;
};


struct Range {
    int start;
    int end;
};
*/

class EagerCompositeKey {
 public:
  uint32_t tableId;
  uint64_t key;

  uint64_t hash;

  EagerCompositeKey(uint32_t table, uint64_t key, uint64_t hash) {
    this->tableId = table;
    this->key = key;
    this->hash = hash;
  }
  
  EagerCompositeKey() {
    this->tableId = 0;
    this->key = 0;
    this->hash = 0;
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

    std::vector<struct EagerRecordInfo> shadowWriteset;
    std::vector<struct EagerRecordInfo> shadowReadset;
    

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

    virtual void AddReadKey(uint32_t tableId, uint64_t key, 
                            uint64_t numRecords) {
      EagerCompositeKey compKey;//(tableId, key);
      compKey.tableId = tableId;
      compKey.key = key;
      compKey.hash = compKey.Hash() % numRecords;
      EagerRecordInfo toAdd;
      toAdd.record = compKey;
      this->readset.push_back(toAdd);
      this->shadowReadset.push_back(toAdd);
    }

    virtual void AddWriteKey(uint32_t tableId, uint64_t key, 
                             uint64_t numRecords) {
      EagerCompositeKey compKey;//(tableId, key);
      compKey.tableId = tableId;
      compKey.key = key;
      compKey.hash = compKey.Hash() % numRecords;
      EagerRecordInfo toAdd;
      toAdd.record = compKey;
      this->writeset.push_back(toAdd);
      this->shadowWriteset.push_back(toAdd);
    }

    virtual void* ReadRef(uint32_t index) {
      return shadowReadset[index].value;
    }
    
    virtual void* WriteRef(uint32_t index) {
      return shadowWriteset[index].value;
    }
};

class RMWEagerAction : public EagerAction {
 public:
  virtual bool Run() {
    if (recordSize == 8) {      // longs
      uint64_t counter = 0;
      uint32_t numReads = shadowReadset.size();
      for (uint32_t i = 0; i < numReads; ++i) {
        uint64_t *record = (uint64_t*)shadowReadset[i].value;
        counter += *record;
      }

      uint32_t numWrites = shadowWriteset.size();
      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *record = (uint64_t*)shadowWriteset[i].value;
        counter += *record;
      }

      for (uint32_t i = 0; i < numWrites; ++i) {
        uint64_t *record = (uint64_t*)shadowWriteset[i].value;
        *record += counter;
      }
    }
    else if (recordSize == 1000) {      //YCSB
      uint32_t numReads = shadowReadset.size();
      uint32_t numWrites = shadowWriteset.size();
      char *read_ptr, *write_ptr;
      uint32_t num_fields = YCSB_RECORD_SIZE / 100;
      uint64_t counter = 0;
      for (uint32_t i = 0; i < numReads; ++i) {
              read_ptr = (char*)shadowReadset[i].value;
              for (uint32_t j = 0; j < num_fields; ++j)
                      counter += *((uint64_t*)&read_ptr[j*100]);
      }
      for (uint32_t i = 0; i < numWrites; ++i) {
              read_ptr = (char*)shadowWriteset[i].value;
              for (uint32_t j = 0; j < num_fields; ++j)
                      counter += *((uint64_t*)&read_ptr[j*100]);
      }
      for (uint32_t i = 0; i < numWrites; ++i) {
              write_ptr = (char*)shadowWriteset[i].value;
              for (uint32_t j = 0; j < num_fields; ++j) 
                      *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
      }
    }      

    return true;
  }
};


#endif // ACTION_H
