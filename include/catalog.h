#ifndef         CATALOG_H_
#define         CATALOG_H_

#include <mv_table.h>
#include <unordered_map>
#include <vector>

using namespace std;

/*
 * The catalog is a mechanism to access the tables in the database. A single 
 * database contains a single instance of the catalog class.
 */
class Catalog
{

 private:
        
  // Store table mappings in this hash table.
  MVTable **tableMappings;
  uint32_t numTables;
  
  // 
  unordered_map<uint32_t, uint64_t> tablePartitionSizes;
  
  volatile uint64_t 
    __attribute__((__packed__, __aligned__(CACHE_LINE))) lockWord;
  
  bool finalized;

 public:
        
  // Constructor.
  Catalog(uint32_t numTables);

  void PutPartition(uint32_t tableId, uint32_t partitionId, 
                    MVTablePartition *partition);

  void Finalize();

  // Add a table to the catalog.
  // 
  // param tableId: A 32-bit unique identifier to associate the table with.
  // param in: A reference to the table we're adding to the catalog.
  //
  // return value: true if the put succeeds, otherwise false.
  //  bool PutTable(uint32_t tableId, MVTable *in);

  // Get a reference to a table.
  //
  // param tableId: A 32-bit unique identifier to associate the table with.
  // param out: We return a reference to the table through this parameter.
  //
  // return value: true if we're able to find a table mapped to the given 
  //                               tableId.
  MVTable* GetTable(uint32_t tableId);     
};

#endif          /* CATALOG_H_ */
