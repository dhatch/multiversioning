#ifndef                 TABLE_H_
#define                 TABLE_H_

#include <city.h>
#include <cpuinfo.h>
#include <cassert>

struct TableRecord {
        struct TableRecord *next;
        uint64_t key;
        char value[0];
};

struct TableConfig {
  uint64_t tableId;
  uint64_t numBuckets;
  int startCpu;
  int endCpu;
  uint64_t freeListSz;
  uint64_t valueSz;
};

class Table {
 private:
  TableRecord **buckets;
  TableRecord *freeList;
  TableConfig  conf;
  bool init;

  inline TableRecord* GetRecord() {
    assert(freeList != NULL);
    TableRecord *ret = freeList;
    freeList = freeList->next;
    ret->next = NULL;
    return ret;
  }

 public:
  void* operator new(std::size_t sz, int cpu) {
          return alloc_mem(sz, cpu);
          //          return malloc(sz);
  }

  void SetInit() {
    this->init = true;
  }

  Table(TableConfig conf) {
    this->init = false;
    this->conf = conf;    
    
    // Initialize hash buckets
    buckets = (TableRecord**)alloc_interleaved(conf.numBuckets*sizeof(TableRecord*),
                                               conf.startCpu, 
                                               conf.endCpu);
    memset(buckets, 0x0, conf.numBuckets*sizeof(TableRecord*));

    // Initialize freelist
    uint32_t recordSz = sizeof(TableRecord)+conf.valueSz;
    char *data = (char*)alloc_interleaved(conf.freeListSz*recordSz,
                                          conf.startCpu, 
                                          conf.endCpu);
    memset(data, 0x0, conf.freeListSz*recordSz);
    for (uint64_t i = 0; i < conf.freeListSz; ++i) {
      ((TableRecord*)(data + i*recordSz))->next = (TableRecord*)(data + (i+1)*recordSz);
    }    
    ((TableRecord*)(data + (conf.freeListSz-1)*recordSz))->next = NULL;    
    freeList = (TableRecord*)data;
  }

  virtual void Put(uint64_t key, void *value) {
    uint64_t index = 
      Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
    TableRecord *rec = GetRecord();
    rec->next = buckets[index];
    rec->key = key;
    memcpy(rec->value, value, conf.valueSz);
    buckets[index] = rec;
  }
  
  virtual void* Get(uint64_t key) {
    uint64_t index = 
      Hash128to64(std::make_pair(conf.tableId, key)) % conf.numBuckets;
    TableRecord *rec = buckets[index];
    while (rec != NULL && rec->key != key) {
      rec = rec->next;
    }
    
    assert(rec != NULL);
    return (void*)(rec->value);
  }

  uint32_t RecordSize()
  {
          return conf.valueSz;
  }
};

#endif          // TABLE_H_
