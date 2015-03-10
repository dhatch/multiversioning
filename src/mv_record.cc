#include <mv_record.h>
#include <cstdlib>
#include <cpuinfo.h>
#include <iostream>

uint64_t _MVRecord_::INFINITY = 0xFFFFFFFFFFFFFFFF;

MVRecordAllocator::MVRecordAllocator(uint64_t size, int cpu, int worker_start, int worker_end) {
        //  std::cout << "NUMA node: " << numa_node_of_cpu(cpu) << "\n";
        worker_start += 1;
        worker_end += 1;
  if (size < 1) {
    size = 1;
  }
  MVRecord *data = (MVRecord*)alloc_mem(size, cpu);
  assert(data != NULL);
  memset(data, 0xA3, size);
        
  this->size = size;
  this->count = 0;
  uint64_t numRecords = size/sizeof(MVRecord);
  
  uint64_t recordDataSize = recordSize*numRecords;
  //  std::cout << "Record size: " << recordSize << "\nRecord data size: " << recordDataSize << "\n";
  //  std::cout << "Worker start:" << worker_start << " Worker end: " << worker_end << "\n";
  //  std::cout << "Cpus: " << numa_num_configured_cpus() << "\n";
  //  char *recordData = NULL;
  //  int cntr = 0;
  //  while (recordData == NULL) {
  //          recordData = (char*)alloc_interleaved(recordDataSize, worker_start, worker_end+cntr*10);
  //          ++cntr;
  //  }
  //  char *recordData = (char*)alloc_interleaved(recordDataSize, worker_start+10, 79);

  //  char *recordData = (char*)alloc_mem(recordDataSize, 19);
  
  char *recordData = (char*)alloc_interleaved_all(recordDataSize);
  //char *recordData = (char*)alloc_mem(recordDataSize, recordCpu);

  //  char *recordData = (char*)alloc_interleaved(recordDataSize, worker_start, worker_end);
  assert(recordData != NULL);
  memset(recordData, 0xA3, recordDataSize);
  //  std::cout << "Done initializing record data\n";
  //  uint64_t endIndex = numRecords-1;
  for (uint64_t i = 0; i < numRecords; ++i) {
    data[i].allocLink = &data[i+1];
    //data[i].value = NULL;
    data[i].value = (Record*)(&recordData[i*recordSize]);
    this->count += 1;
  }
  data[numRecords-1].allocLink = NULL;
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
  ret->epoch_ancestor = NULL;
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



