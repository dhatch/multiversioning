#include <catalog.h>
#include <util.h>
#include <preprocessor.h>

Catalog::Catalog() {
  finalized = false;
}

void Catalog::PutPartition(uint32_t tableId, uint32_t partitionId, 
                           MVTablePartition *partition) {
  lock(&lockWord);
  assert(!finalized);

  // Check whether we've seen this table before.
  MVTable *tbl;
  auto entry = tableMappings.find(tableId);
  if (entry == tableMappings.end()) {    
    // Haven't seen the table. Create a new one.
    tbl = new MVTable(MVScheduler::NUM_CC_THREADS);
    tableMappings[tableId] = tbl;
  }
  else {
    // We've seen the table before.    
    tbl = (*entry).second;
  }

  // Add the partition to the table.
  tbl->AddPartition(partitionId, partition);
  unlock(&lockWord);
}

void Catalog::Finalize() {
  lock(&lockWord);
  finalized = true;
  unlock(&lockWord);
}

/*
 * If the given tableId already exists, preserve the old mapping and return 
 * false. Otherwise, this call succeeds.
 */
bool Catalog::PutTable(uint32_t tableId, MVTable *in) {
  if (tableMappings.find(tableId) == tableMappings.end()) {
    tableMappings[tableId] = in;
    return true;
  }
  return false;
}

/*
 * If the given tableId exists, return true, otherwise, return false.
 */
bool Catalog::GetTable(uint32_t tableId, MVTable **out) {
  auto iter = tableMappings.find(tableId);
  if (iter == tableMappings.end()) {
    *out = NULL;
    return false;
  }
  else {
    *out = iter->second;
    return true;
  }
}
