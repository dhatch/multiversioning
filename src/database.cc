#include <database.h>

Database::Database(uint32_t numTables) {
  this->catalog = new Catalog(numTables);
}

void Database::PutPartition(uint32_t tableId, uint32_t partitionNumber, 
                            MVTablePartition *partition) {
  catalog->PutPartition(tableId, partitionNumber, partition);
}

MVTable* Database::GetTable(uint32_t tableId) {
  return catalog->GetTable(tableId);
}

