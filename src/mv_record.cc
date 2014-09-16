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
	freeList = data;
}

bool MVRecordAllocator::GetRecord(MVRecord **out) {
	if (freeList == NULL) {
		*out = NULL;
		return false;
	}
	
	MVRecord *ret = freeList;
	freeList = freeList->recordLink;

	// Set up the MVRecord to return.
	memset(ret, 0xA3, sizeof(MVRecord));
	ret->link = NULL;
	ret->recordLink = NULL;
	
	*out = ret;
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


