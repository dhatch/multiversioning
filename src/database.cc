#include <database.h>

Database::Database() {
}

void Database::PutPartition(uint32_t tableId, uint32_t partitionNumber, 
                            MVTablePartition *partition) {
  catalog.PutPartition(tableId, partitionNumber, partition);
}

bool Database::PutTable(uint32_t tableId, MVTable *in) {
  return catalog.PutTable(tableId, in);
}

bool Database::GetTable(uint32_t tableId, MVTable **out) {
  return catalog.GetTable(tableId, out);
}
