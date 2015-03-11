#ifndef         MV_TABLE_H_
#define         MV_TABLE_H_

#include <mv_record.h>
#include <mv_action.h>


/*
 * Single-writer hash table. Each scheduler thread contains a unique 
 * MVTablePartition for every table in the system.
 */
class MVTablePartition {
        
 private:
  MVRecordAllocator *allocator;
  uint64_t numSlots;
  MVRecord **tableSlots;
        
 public:

      void* operator new (std::size_t sz, int cpu) {
            return alloc_mem(sz, cpu);
      }
  // Constructor. 
  //
  // param size: Number of slots in the hash table.
  // param alloc: Allocator to use for creating MVRecords.
  MVTablePartition(uint64_t size, int cpu, MVRecordAllocator *alloc);     
        
  // Get the latest version for the given primary key. If we're unable to find
  // a live instance of the record, return false. Otherwise, return true.
  //
  // param pkey: Primary key of record we wish to read.
  // param version: The most recent version of the record.
  // 
  // return value: true if the lookup finds a live instance of the record. 
  //                               Otherwise, false.
  //  bool GetLatestVersion(CompositeKey pkey, uint64_t *version);

  // Insert a new version for the given record.
  //
  // param pkey: Primary key of the record being written.
  // param action: Reference to the transaction that will create the record.
  // param version: Version of the record to write out.
  // 
  // return value: true if the write is successful, false otherwise. 
  bool WriteNewVersion(CompositeKey &pkey, Action *action, uint64_t version);


  MVRecord* GetMVRecord(const CompositeKey &pkey, uint64_t version);

  
  
  //  void WritePartition();
        
  //  MVRecordAllocator* GetAlloc();
};

/*
 * MVTable is just a convenience class that wraps on top of MVTablePartition.
 * Scheduler and worker threads do not directly call MVTablePartition table 
 * directly. 
 */
class MVTable {

 private:
        
  // Array of pointers to table partitions.
  MVTablePartition **tablePartitions;
        
  // Total number of entries in the tablePartitions array.
  uint32_t numPartitions;

 public:
        
  // Constructor.
  //
  // param numPartitions: The total number of partitions in the system. 
  // param partitions: An array of pointers to each table partition.
  MVTable(uint32_t numPartitions);
        
  // 
  //
  //
  void AddPartition(uint32_t partitionId, MVTablePartition *partition);

  // Scheduler threads call this function in order to obtain the latest 
  // version of a particular record.
  //
  // param partition: Which partition to search.
  // param pkey: Primary key of the record we're looking for.
  // param version: Pass the version we're returning through this pointer.
  // 
  // return value: true if we're able to find a live version. otherwise, 
  //                               false.
  /*
  bool GetLatestVersion(uint32_t partition, const CompositeKey& pkey, 
                        uint64_t *version);
  */

  // Scheduler threads call this function in order to create a new version for
  // a particular record.
  // 
  // param partition: Which partition to search
  // param pkey: Primary key of the record we're looking for.
  // param action: The transaction responsible for producing the new record.
  // param version: The creation timestamp of the new record.
  // 
  // return value: true if we're able to insert a new version for the record. 
  //                               May fail if the allocator runs out of memory.
  bool WriteNewVersion(uint32_t partition, CompositeKey& pkey, 
                       Action *action, uint64_t timestamp);   

  MVTablePartition* GetPartition(uint32_t partition) {
    return tablePartitions[partition];
  }
  
  MVRecord* GetMVRecord(uint32_t partition, const CompositeKey &pkey, 
                        uint64_t version);
};

#endif          /* MV_TABLE_H_ */
