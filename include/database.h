#ifndef         DATABASE_H_
#define         DATABASE_H_

#include <catalog.h>

/*
 * The methods in this class are _not_ thread-safe. We anticipate that the class
 * will be read-only after initialization.
 */
class Database {

 private:
  Catalog *catalog;

 public:
  Database(uint32_t numTables);     

  void PutPartition(uint32_t tableId, uint32_t partitionNumber, 
                    MVTablePartition *partition);
        
  // Wrapper around Catalog's PutTable method (include/catalog.h).
  //bool PutTable(uint32_t tableId, MVTable *in);
        
  // Wrapper around Catalog's GetTable method (include/catalog.h).
  MVTable* GetTable(uint32_t tableId);
  //  bool GetTable(uint32_t tableId, MVTable **out); 
};

extern Database DB;

#endif          /* DATABASE_H_ */
