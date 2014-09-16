#ifndef 	PREPROCESSOR_H_
#define 	PREPROCESSOR_H_

#include <runnable.hh>

class BufferArithmetic {
 public:
	
	static void* GetLastElement(void *buf, uint64_t bufferSize, 
								 uint64_t elementSize);
	static void* GetElementAt(void *buf, uint64_t bufferSize, 
							  uint64_t elementSize, uint64_t index);
};

class VersionBuffer;
class CompositeKey;
class Action;

class VersionBufferAllocator {	

	friend class VBufAllocatorTest;

 private:
	void *freeList;

 public:
	static const uint32_t BUFFER_SIZE = 128;
	VersionBufferAllocator(uint64_t totalSize);
	bool GetBuffer(void **outBuf);
	void ReturnBuffers(VersionBuffer *buffer);
};

class VersionBuffer
{
	friend class VersionBufferAllocator;
	friend class VBufAllocatorTest;
	friend class MVScheduler;

 private:	
	// Each version buffer consists of several 64-byte buffers linked together.
	// the pointer "data" points to the first of these 64-byte buffers.
	void 		*head;
	void 		*tail;
	
	int offset;
	VersionBufferAllocator *alloc;
	
 public:
	
	// Hack: Each buffer contains 6 CompositeKeys.
	static const uint32_t NUM_ELEMS = 6;

	VersionBuffer();
	
	// Initialize an empty version buffer.
	VersionBuffer(VersionBufferAllocator *alloc);
	
	// Appends "link" to the data.
	void AddLink(void *link);
	
	// Move the position of the cursor to the next element in the buffer.
	bool Move();
	
	bool Append(CompositeKey key);

	void Write(uint64_t value);

	// Move the position of the cursor back to the first element of the buffer.
	void Reset();
	
	uint64_t GetValue();
};

/*
 * MVScheduler implements scheduling logic. The scheduler is partitioned across 
 * several physical cores.
 */
class MVScheduler : public Runnable {
	friend class SchedulerTest;
	
 private:
	static uint32_t NUM_CC_THREADS;
	static inline uint32_t GetCCThread(CompositeKey key);
	
	uint32_t threadId;
	VersionBufferAllocator *alloc;

	uint32_t epoch;
	uint32_t txnCounter;

 protected:
	virtual void StartWorking();
	void ProcessReadset(Action *action);
	void ProcessWriteset(Action *action, uint64_t timestamp);
	void ScheduleTransaction(Action *action);	

 public:
	MVScheduler(int cpuNumber, uint32_t threadId, uint64_t totalBufSize);	
};


#endif 		/* PREPROCESSOR_H_ */
