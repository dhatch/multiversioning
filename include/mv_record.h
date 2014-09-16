#ifndef 	MV_RECORD_H_
#define 	MV_RECORD_H_

#include <action.h>

typedef struct _MVRecord_ MVRecord;

struct _MVRecord_ {
	
	// The unique creation timestamp
	uint64_t createTimestamp;
	
	// The time at which this record became invalid.
	uint64_t deleteTimestamp;
	
	CompositeKey key;
	
	// The transaction responsible for creating a value associated with the 
	// record.
	Action *writer;
	
	// The actual value of the record.
	void *value;
	
	// 
	MVRecord *link;
	
	MVRecord *recordLink;
} __attribute__((__packed__));

/*
 * MVRecords are returned to the allocator (defined below) in bulk using this 
 * data structure. 
 */
struct MVRecordList {
	MVRecord *head;
	MVRecord *tail;
};

/*
 * Each scheduler thread contains a reference to a unique instance of 
 * MVRecordAllocator. Each thread's reference is unique, and therefore 
 * not shared across multiple threads. 
 */
class MVRecordAllocator {

	friend class MVAllocatorTest;
	
 private:
	MVRecord *freeList;

 public:
	
	// Constructor takes a size parameter, which is the total number of bytes 
	// allocator can work with.
	MVRecordAllocator(uint64_t size);
	
	// 
	bool GetRecord(MVRecord **out);
	void ReturnMVRecords(MVRecordList recordList);	
};

#endif 		/* MV_RECORD_H_ */
