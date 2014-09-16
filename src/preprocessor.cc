#include <preprocessor.h>
#include <machine.h>
#include <city.h>
#include <action.h>
#include <catalog.h>
#include <database.h>

#include <stdlib.h>

#include <cassert>
#include <cstring>
#include <deque>

using namespace std;

uint32_t MVScheduler::NUM_CC_THREADS = 1;

/*
 * Given a buffer, get a pointer to the last element in the buffer.
 * This function requires the size of the buffer, and the size of each 
 * element in the buffer. bufferSize is the size of the buffer in bytes.
 * elementSize is the size of an element in bytes.
 */
inline  void* BufferArithmetic::GetLastElement(void *buf, uint64_t bufferSize, 
											   uint64_t elementSize) {
	assert(bufferSize > elementSize);
	assert(bufferSize % elementSize == 0);		
		
	uint64_t numElems = bufferSize/elementSize;
	uint64_t elemIndex = numElems-1;
	uint64_t elemByteOffset = elementSize*elemIndex;
	char *bufPtr = (char *)buf;
	char *ret = &bufPtr[elemByteOffset];
	return ret;
}	

inline void* BufferArithmetic::GetElementAt(void *buf, uint64_t bufferSize, 
											uint64_t elementSize, 
											uint64_t index) {
	assert(bufferSize > elementSize);
	assert(bufferSize % elementSize == 0);
	assert(index < (bufferSize/elementSize));
		
	uint64_t elemByteOffset = elementSize*index;
	char *bufPtr = (char *)buf;
	char *ret = &bufPtr[elemByteOffset];
	return ret;
}

/*  
 * totalSize: The total amount of memory the BufferAllocator has at its disposal
 * bufferSize: The amount of memory allocated per chunk.
 */ 
VersionBufferAllocator::VersionBufferAllocator(uint64_t totalSize) {
    // Make sure that the sizes we've been given are reasonable.
    assert((totalSize % BUFFER_SIZE) == 0);
    assert((BUFFER_SIZE % sizeof(uint64_t)) == 0);
    assert((BUFFER_SIZE % CACHE_LINE) == 0);

    // Can't deal with failures. If we fail to allocate the required memory,
    // fail, and restart the program with an appropriately sized buffer.
    void *data = malloc(totalSize);
    assert(data != NULL);
    
    // Set all the bytes to garbage.
    memset(data, 0xA3, totalSize);
    
    // Thread the free-list through the buffers.
    char *charData = (char*)data;
    uint64_t numBuffers = totalSize / BUFFER_SIZE;   
    for (uint64_t i = 0; i < numBuffers ; ++i) {
        
        // Get the address of the current buffer and the buffer after the 
        // current buffer.
        void *curBuf = (void*)(charData + BUFFER_SIZE*i);
        void *nextBuf = (void*)(charData + BUFFER_SIZE*(i+1));
        
        // Make the link pointer in the current buffer point to the next buffer.
        void *linkPtr = BufferArithmetic::GetLastElement(curBuf, BUFFER_SIZE, 
                                                         sizeof(uint64_t));
        *((uint64_t*)linkPtr) = (uint64_t)nextBuf;
    }
    
    // Set the link pointer in the last buffer to NULL.
    void *lastBuf = (void*)(charData + BUFFER_SIZE*(numBuffers-1));
    void *linkPtr = BufferArithmetic::GetLastElement(lastBuf, BUFFER_SIZE, 
                                                     sizeof(uint64_t));
    *((uint64_t*)linkPtr) = (uint64_t)NULL;
    
    // Finally set our freeList pointer to point to the list of initialized 
    // buffers.
    freeList = data;
}

/*
 * Return a free buffer to the caller. If we find a free buffer, return true. If
 * we can't, return false.
 */
bool VersionBufferAllocator::GetBuffer(void **outBuf)
{
    // No more free buffers, return false.
    if (freeList == NULL) {
        return false;
    }
    
    // Set outbuf appropriately. Make freeList point to the next free buffer.
    *outBuf = freeList;
	void *linkPtr = BufferArithmetic::GetLastElement(freeList, BUFFER_SIZE, 
													 sizeof(uint64_t));
	freeList = *((void **)linkPtr);
	*((void **)linkPtr) = NULL;
    return true;
}

/*
 * Mark a list of buffers in the VersionBuffer as being unused.  
 */
void VersionBufferAllocator::ReturnBuffers(VersionBuffer *buf)
{
    if (buf->tail != NULL) {
        
        // Set the tail's link pointer to the current head of the free list.
        void *linkPtr = BufferArithmetic::GetLastElement(buf->tail, 
														 BUFFER_SIZE, 
                                                         sizeof(uint64_t));
        *((uint64_t*)linkPtr) = (uint64_t)freeList;

        // Set the free list to the head.
        freeList = buf->head;
    }
    else {
        assert(buf->head == NULL && buf->tail == NULL);
    }
	buf->head = NULL;
	buf->tail = NULL;
	buf->offset = 0;	
}

VersionBuffer::VersionBuffer() {
	this->head = NULL;
	this->tail = NULL;
	this->offset = 0;
	this->alloc = NULL;
}

VersionBuffer::VersionBuffer(VersionBufferAllocator *alloc)
{
	this->head = NULL;
	this->tail = NULL;
	this->offset = 0;
	this->alloc = alloc;
}

// We require that link is of size CACHE_LINE_SIZE.
void VersionBuffer::AddLink(void *link) {

    // The size of the link buffer is equal to the size of a cache line. Set
    // all the bytes except the last 8 to garbage.
	memset(link, 0xA3, VersionBufferAllocator::BUFFER_SIZE);

    // The last 8 bytes in the link field point to the next link. This is the
    // last link to be added, therefore, we set the pointer to NULL.
	void *linkPtr = BufferArithmetic::
		GetLastElement(link, VersionBufferAllocator::BUFFER_SIZE, 
					   sizeof(uint64_t));
	*((uint64_t**)linkPtr) = NULL;

    // Add the new link to the chain of buffers.
    if (this->head == NULL) {
		
		// The version buffer is currently empty.
        assert(this->tail == NULL);
        head = link;
        tail = link;
    }
    else {
		
		// Make the current tail's link pointer point to the new link.
		void *tailLink = BufferArithmetic::
			GetLastElement(tail, VersionBufferAllocator::BUFFER_SIZE,
						   sizeof(uint64_t));
		*((void**)tailLink) = link;
		
		// Set the new link as the tail.
		tail = link;
    }
}

/*
 * Add a new key to the version buffer. If there isn't enough space, return 
 * false. The caller should ensure that there are enough buffers and try again.
 */
bool VersionBuffer::Append(CompositeKey value) {
	
	// Ensure that the current offset is valid.
	assert(NUM_ELEMS == (int)NUM_ELEMS);
	assert(offset < (int)NUM_ELEMS && offset >= -1);
	offset += 1;
	
	// offset == NUM_ELEMS means that we've filled up the current buffer. Try 
	// to allocate a new buffer. If the allocation fails, return false.
	if (offset == NUM_ELEMS || tail == NULL) {
		void *buffer;
		if (alloc->GetBuffer(&buffer)) {

			// New buffer successfully allocated. Link the new buffer to the 
			// version buffer and begin from offset 0.
			AddLink(buffer);
			offset = 0;
		}
		else {

			// Allocation failed.			
			offset -= 1;
			return false;
		}
	}
	
	// Success. Write the value to the appropriate location in the version 
	// buffer.
	assert(offset < (int)NUM_ELEMS);
	((CompositeKey*)tail)[offset] = value;
	return true;
}

MVScheduler::MVScheduler(int cpuNumber, uint32_t threadId, 
						 uint64_t totalBufSize) : Runnable(cpuNumber) {
	this->threadId = threadId;
	this->alloc = new VersionBufferAllocator(totalBufSize);
}

void MVScheduler::StartWorking() {

}

/*
 * For each record in the readset, find out the version which should be read
 * by this transaction. We always pick the latest version.
 */
void MVScheduler::ProcessReadset(Action *action) {
	VersionBuffer readBuffer(alloc);
	for (uint32_t i = 0; i < action->readset.size(); ++i) {
		CompositeKey record = action->readset[i];
		uint64_t version;
		if (GetCCThread(record) == threadId) {
			
			// Try to get a reference to the table from the catalog. Ensure that
			// the table actually exists.
			MVTable *tbl;
			if (!DB.GetTable(record.tableId, &tbl)) {
				assert(false);
			}
			
			// XXX What if the table does not contain the record?
			if (!tbl->GetLatestVersion(threadId, record, &version)) {
				assert(false);
			}
		}
		record.version = version;
		bool appendSuccess = readBuffer.Append(record);
		assert(appendSuccess);
		action->readset[i].version = version;
	}
	action->readVersions[threadId] = readBuffer;
}

/*
 * Hash the given key, and find which concurrency control thread is
 * responsible for the appropriate key range. 
 */
uint32_t MVScheduler::GetCCThread(CompositeKey key) {
	uint64_t hash = CompositeKey::Hash(&key);
	return (uint32_t)(hash % NUM_CC_THREADS);
}


/*
 * For each record in the writeset, write out a placeholder indicating that
 * the value for the record will be produced by this transaction. We don't need
 * to track the version of each record written by the transaction. The version
 * is equal to the transaction's timestamp.
 */
void MVScheduler::ProcessWriteset(Action *action, uint64_t timestamp) {
	for (uint32_t i = 0; i < action->writeset.size(); ++i) {
		CompositeKey record = action->writeset[i];
		uint64_t version;
		if (GetCCThread(record) == threadId) {
			
			MVTable *tbl;
			if (!DB.GetTable(record.tableId, &tbl)) {
				assert(false);
			}
			
			if (!tbl->WriteNewVersion(threadId, record, action, timestamp)) {
				assert(false);
			}
		}
	}
}


void MVScheduler::ScheduleTransaction(Action *action) {
	txnCounter += 1;
	if (threadId == 0) {
		uint64_t version = (((uint64_t)epoch << 32) | txnCounter);
		action->version = version;
	}
	ProcessReadset(action);
	ProcessWriteset(action, action->version);
}
