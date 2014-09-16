#include <mv_table.h>

MVTable::MVTable(uint32_t numPartitions, MVTablePartition **partitions) {
	this->numPartitions = numPartitions;
	this->tablePartitions = partitions;
}

bool MVTable::GetLatestVersion(uint32_t partition, CompositeKey pkey, 
					  uint64_t *version) {
	assert(partition < numPartitions);	// Validate that the partition is valid.
	return tablePartitions[partition]->GetLatestVersion(pkey, version);
}

bool MVTable::WriteNewVersion(uint32_t partition, CompositeKey pkey, 
							  Action *action, uint64_t version) {
	assert(partition < numPartitions);	// Validate that the partition is valid.
	return tablePartitions[partition]->WriteNewVersion(pkey, action, version);
}

MVTablePartition::MVTablePartition(uint64_t size, 
								   MVRecordAllocator *alloc) {
	this->numSlots = size;
	this->allocator = alloc;
	
	// Allocate a contiguous chunk of memory in which to store the table's slots
	this->tableSlots = (MVRecord**)malloc(sizeof(MVRecord*)*size);
	assert(this->tableSlots != NULL);
	memset(this->tableSlots, 0x00, sizeof(MVRecord*)*size);	
}

/*
 * Given a primary key, find the slot associated with the key. Then iterate 
 * through the hash table's bucket list to find the key.
 */
bool MVTablePartition::GetLatestVersion(CompositeKey pkey, 
										  uint64_t *version) {	
	uint64_t slotNumber = CompositeKey::Hash(&pkey) % numSlots;
	MVRecord *hashBucket = tableSlots[slotNumber];
	while (hashBucket != NULL) {
		if (hashBucket->key == pkey && hashBucket->deleteTimestamp == 0) {
			*version = hashBucket->createTimestamp;
			return true;
		}
		hashBucket = hashBucket->link;
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
	assert(success);	// Can't deal with allocation failures yet.
	assert(toAdd->link == NULL && toAdd->recordLink == NULL);
	toAdd->createTimestamp = version;
	toAdd->deleteTimestamp = 0;
	toAdd->writer = action;
	toAdd->key = pkey;	

	// Get the slot number the record hashes to, and try to find if a previous
	// version of the record already exists.
	uint64_t slotNumber = CompositeKey::Hash(&pkey) % numSlots;
	MVRecord *cur = tableSlots[slotNumber];
	MVRecord **prev = &tableSlots[slotNumber];

	while (cur != NULL) {
		
		// We found the record. Link to the old record.
		if (cur->key == pkey) {
			break;
		}

		prev = &cur->link;
		cur = cur->link;
	}
	
	// Link in the MVRecord.
	if (cur != NULL) {
		toAdd->link = cur->link;
		toAdd->recordLink = cur;
		cur->link = NULL;
	}
	*prev = toAdd;
	return true;
}
