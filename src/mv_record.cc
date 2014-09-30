#include <mv_record.h>
#include <cstdlib>
#include <cpuinfo.h>
#include <iostream>

MVRecordAllocator::MVRecordAllocator(uint64_t size, int cpu) {
    std::cout << "NUMA node: " << numa_node_of_cpu(cpu) << "\n";
    MVRecord *data = (MVRecord*)alloc_mem(size, cpu);
  assert(data != NULL);
  memset(data, 0xA3, size);
        
  this->size = size;
  uint64_t numRecords = size/sizeof(MVRecord);
  uint64_t endIndex = numRecords-1;
  for (uint64_t i = 0; i < numRecords/2; ++i) {
    data[i].recordLink = &data[endIndex-i];
    data[endIndex-i].recordLink = &data[i+1];
  }
  data[numRecords/2].recordLink = NULL;
  freeList = data;
}

void MVRecordAllocator::WriteAllocator() {
  memset(freeList, 0x00, size);
  MVRecord *data = freeList;
  uint64_t numRecords = size/sizeof(MVRecord);
  uint64_t endIndex = numRecords-1;
  for (uint64_t i = 0; i < numRecords/2; ++i) {
    data[i].recordLink = &data[endIndex-i];
    data[endIndex-i].recordLink = &data[i+1];
  }
  data[numRecords/2].recordLink = NULL;
  
}

bool MVRecordAllocator::GetRecord(MVRecord **OUT_recordPtr) {
  if (freeList == NULL) {
    *OUT_recordPtr = NULL;
    return false;
  }
        
  MVRecord *ret = freeList;
  freeList = freeList->recordLink;

  // Set up the MVRecord to return.
  //  memset(ret, 0xA3, sizeof(MVRecord));
  ret->link = NULL;
  ret->recordLink = NULL;
        
  *OUT_recordPtr = ret;
  return true;
}

/*
 *
 */
void MVRecordAllocator::ReturnMVRecords(MVRecordList recordList) {
        
  // XXX Should we validate that MVRecords are properly linked?
  if (recordList.tail != NULL) {
    (recordList.tail)->link = freeList;
    freeList = recordList.head;
  }
  else {
    assert(recordList.head == NULL);
  }
}



