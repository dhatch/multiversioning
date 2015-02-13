#ifndef         MV_RECORD_H_
#define         MV_RECORD_H_

#include <stdint.h>
#include <cassert>
#include <cstddef>
#include <cpuinfo.h>
#include <iostream>

class Action;
class Record;

extern uint64_t recordSize;

typedef struct _MVRecord_ MVRecord;


struct _MVRecord_ {
  
  static uint64_t INFINITY;        

  uint64_t createTimestamp;
  uint64_t deleteTimestamp;
  uint64_t key;
        
  // The transaction responsible for creating a value associated with the 
  // record.
  Action *writer;
        
  // The actual value of the record.
  Record *value;        

  MVRecord *link;        
  MVRecord *recordLink;
        MVRecord *epoch_ancestor;
  MVRecord *allocLink;

  uint32_t writingThread;
} __attribute__((__packed__));

/*
 * MVRecords are returned to the allocator (defined below) in bulk using this 
 * data structure. 
 */
struct MVRecordList {
  MVRecord *head;
  MVRecord **tail;
  uint64_t count;
};

/*
 * Each scheduler thread contains a reference to a unique instance of 
 * MVRecordAllocator. Each thread's reference is unique, and therefore 
 * not shared across multiple threads. 
 */
class MVRecordAllocator {

  friend class MVAllocatorTest;
        
 private:
  MVRecord *freeList;
  uint64_t count;
  uint64_t size;
 public:

  void* operator new(std::size_t sz, int cpu) {
          return alloc_mem(sz, cpu);
          /*
    int numa_node = numa_node_of_cpu(cpu);
    numa_set_strict(1);
    void *buf = numa_alloc_onnode(sz, numa_node);
    if (buf == NULL) {
      return buf;
    }
    if (mlock(buf, sz) != 0) {
      numa_free(buf, sz);
      std::cout << "mlock couldn't pin memory to RAM!\n";
      return NULL;
    } 
    else {
      return buf;
    }
          */
  };
        
  // Constructor takes a size parameter, which is the total number of bytes 
  // allocator can work with.
  MVRecordAllocator(uint64_t size, int cpu, int worker_start, int worker_end);
        
  // 
  bool GetRecord(MVRecord **out);
  void ReturnMVRecords(MVRecordList recordList);  
  void WriteAllocator();

  inline bool Warning() {
    return count < 128;
  }
};

#endif          /* MV_RECORD_H_ */
