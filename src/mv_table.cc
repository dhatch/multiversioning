#include <mv_table.h>
#include <cpuinfo.h>
#include <iostream>

MVTable::MVTable(uint32_t numPartitions) {
  this->numPartitions = numPartitions;
  this->tablePartitions = (MVTablePartition**)malloc(numPartitions*
                                                     sizeof(MVTablePartition*));
}

void MVTable::AddPartition(uint32_t partitionId, MVTablePartition *partition) {
  assert(partitionId < numPartitions);
  this->tablePartitions[partitionId] = partition;
}

bool MVTable::GetVersion(uint32_t partition, const CompositeKey &pkey, 
                         uint64_t version, Record *OUT_REC) {
  assert(partition < numPartitions);
  return tablePartitions[partition]->GetVersion(pkey, version, OUT_REC);
}

bool MVTable::GetLatestVersion(uint32_t partition, const CompositeKey &pkey, 
                               uint64_t *version) {
  assert(partition < numPartitions);      // Validate that partition is valid.
  
  return tablePartitions[partition]->GetLatestVersion(pkey, version);
}

bool MVTable::WriteNewVersion(uint32_t partition, const CompositeKey &pkey, 
                              Action *action, uint64_t version) {
  assert(partition < numPartitions);      // Validate that partition is valid.
  return tablePartitions[partition]->WriteNewVersion(pkey, action, version);
}

MVTablePartition::MVTablePartition(uint64_t size, 
                                   int cpu,
                                   MVRecordAllocator *alloc) {
  this->numSlots = size;
  this->allocator = alloc;
        
  // Allocate a contiguous chunk of memory in which to store the table's slots
  this->tableSlots = (MVRecord**)alloc_mem(sizeof(MVRecord*)*size, cpu);
  assert(this->tableSlots != NULL);
  memset(this->tableSlots, 0x0, sizeof(MVRecord*)*size); 
  std::cout << "AHAHA\n";
  std::cout << "asldkjfasdf\n";
}

bool MVTablePartition::GetVersion(const CompositeKey &pkey, uint64_t version, 
                                  Record *OUT_rec) {
  // Get the slot number the record hashes to, and try to find if a previous
  // version of the record already exists.
  uint64_t slotNumber = CompositeKey::Hash(&pkey) % numSlots;
  MVRecord *cur = tableSlots[slotNumber];

  while (cur != NULL) {
                
    // We found the record. Link to the old record.
    if (cur->key == pkey.key) {
      while (cur != NULL && cur->deleteTimestamp > version) {
        // Found a valid version
        if (cur->createTimestamp <= version && cur->deleteTimestamp > version) {
          
          // Check if the version has already been substantiated. If 
          // substantiated, "writer" is set to NULL.
          if (cur->writer == NULL) {
            OUT_rec->isMaterialized = true;
            OUT_rec->rec = cur->value;
          }
          else {
            OUT_rec->isMaterialized = false;
            OUT_rec->rec = cur->writer;
          }
          return true;
        }
        cur = cur->recordLink;
      }
      break;
    }
    cur = cur->link;
  }
  return false;
}

/*
void MVTablePartition::WritePartition() {
  memset(tableSlots, 0x00, sizeof(MVRecord*)*numSlots);
}

MVRecordAllocator* MVTablePartition::GetAlloc() {
  return allocator;
}
*/


/*
 * Given a primary key, find the slot associated with the key. Then iterate 
 * through the hash table's bucket list to find the key.
 */
bool MVTablePartition::GetLatestVersion(CompositeKey pkey, 
                                        uint64_t *version) {    

  uint64_t slotNumber = CompositeKey::Hash(&pkey) % numSlots;
  MVRecord *hashBucket = tableSlots[slotNumber];
  while (hashBucket != NULL) {
    if (hashBucket->key == pkey.key) {
      break;
    }
    hashBucket = hashBucket->link;
  }
  if (hashBucket != NULL && hashBucket->deleteTimestamp == 0) {
    *version = hashBucket->createTimestamp;
    return true;
  }
  *version = 0;
  return false;
}

/*
 * Write out a new version for record pkey.
 */
bool MVTablePartition::WriteNewVersion(CompositeKey pkey, Action *action, 
                                       uint64_t version) {

  // Allocate an MVRecord to hold the new record.
  MVRecord *toAdd;
  bool success = allocator->GetRecord(&toAdd);
  assert(success);        // Can't deal with allocation failures yet.
  assert(toAdd->link == NULL && toAdd->recordLink == NULL);
  toAdd->createTimestamp = version;
  toAdd->deleteTimestamp = version;// MVRecord::INFINITY;
  toAdd->writer = action;
  toAdd->key = pkey.key;  

  // Get the slot number the record hashes to, and try to find if a previous
  // version of the record already exists.
  uint64_t slotNumber = CompositeKey::Hash(&pkey) % numSlots;
  MVRecord *cur = tableSlots[slotNumber];
  MVRecord **prev = &tableSlots[slotNumber];

  while (cur != NULL) {
                
    // We found the record. Link to the old record.
    if (cur->key == pkey.key) {
      toAdd->link = cur->link;
      toAdd->recordLink = cur;
      //      cur->deleteTimestamp = version;
      break;
    }

    prev = &cur->link;
    cur = cur->link;
  }
  *prev = toAdd;
  return true;
}
