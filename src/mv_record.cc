#include <mv_record.h>
#include <cstdlib>

MVRecordAllocator::MVRecordAllocator(uint64_t size) {
	assert(size % sizeof(MVRecord) == 0);
	
	MVRecord *data = (MVRecord*)malloc(size);
	assert(data != NULL);
	memset(data, 0xA3, size);
	
	uint64_t numRecords = size/sizeof(MVRecord);
	for (uint64_t i = 0; i < numRecords; ++i) {
		data[i].recordLink = &data[i+1];
	}
	data[numRecords-1].recordLink = NULL;
}

bool MVRecordAllocator::GetRecord(MVRecord **out) {
	if (freeList == NULL) {
		return false;
	}
	
	*out = freeList;
	freeList = freeList->recordLink;
	(*out)->recordLink = NULL;
	(*out)->link = NULL;
	return true;
}

/*
 *
 */
void MVRecordAllocator::ReturnMVRecords(MVRecordList recordList) {
	
	// XXX Should we validate that MVRecords are properly linked?
	if (recordList.tail != NULL) {
		(recordList.tail)->link = freeList;
		freeList = recordList.head;
	}
	else {
		assert(recordList.head == NULL);
	}
}


