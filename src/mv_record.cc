#include <mv_record.h>
#include <cstdlib>
#include <cpuinfo.h>
#include <iostream>

uint64_t _MVRecord_::INFINITY = 0xFFFFFFFFFFFFFFFF;

MVRecordAllocator::MVRecordAllocator(uint64_t size, int cpu) {
    std::cout << "NUMA node: " << numa_node_of_cpu(cpu) << "\n";
    MVRecord *data = (MVRecord*)alloc_mem(size, cpu);
  assert(data != NULL);
  memset(data, 0xA3, size);
        
  this->size = size;
  this->count = 0;
  uint64_t numRecords = size/sizeof(MVRecord);
  
  uint64_t recordDataSize = recordSize*numRecords;
  std::cout << "Record size: " << recordSize << "\nRecord data size: " << recordDataSize << "\n";
  char *recordData = (char*)alloc_mem(recordDataSize, cpu);
  assert(recordData != NULL);
  memset(recordData, 0xA3, recordDataSize);

  uint64_t endIndex = numRecords-1;
  for (uint64_t i = 0; i < numRecords; ++i) {
    data[i].allocLink = &data[i+1];
    data[i].value = (Record*)(&recordData[i*recordSize]);
    this->count += 1;
  }
  data[numRecords-1].allocLink = NULL;
  /*
  for (uint64_t i = 0; i < numRecords/2; ++i) {
    data[i].recordLink = &data[endIndex-i];
    data[endIndex-i].recordLink = &data[i+1];
    count += 2;
  }
  data[numRecords/2].recordLink = NULL;
  */

  freeList = data;
}

void MVRecordAllocator::WriteAllocator() {
  /*
  memset(freeList, 0x00, size);
  MVRecord *data = freeList;
  uint64_t numRecords = size/sizeof(MVRecord);
  uint64_t endIndex = numRecords-1;
  for (uint64_t i = 0; i < numRecords/2; ++i) {
    data[i].recordLink = &data[endIndex-i];
    data[endIndex-i].recordLink = &data[i+1];
  }
  data[numRecords/2].recordLink = NULL;
  */
}

bool MVRecordAllocator::GetRecord(MVRecord **OUT_recordPtr) {
  if (freeList == NULL) {
    std::cout << "Free list empty: " << count << "\n";
    *OUT_recordPtr = NULL;
    return false;
  }
        
  MVRecord *ret = freeList;
  freeList = freeList->allocLink;

  // Set up the MVRecord to return.
  //  memset(ret, 0xA3, sizeof(MVRecord));
  ret->link = NULL;
  ret->recordLink = NULL;
  ret->allocLink = NULL;
        
  *OUT_recordPtr = ret;
  count -= 1;
  return true;
}

/*
 *
 */
void MVRecordAllocator::ReturnMVRecords(MVRecordList recordList) {
        
  // XXX Should we validate that MVRecords are properly linked?
  if (recordList.tail != &recordList.head) {
    *(recordList.tail) = freeList;
    freeList = recordList.head;
    this->count += recordList.count;
  }
  else {
    assert(recordList.head == NULL);
  }
}



