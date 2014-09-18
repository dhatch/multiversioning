#include <database.h>
#include <preprocessor.h>
#include <gtest/gtest.h>
#include <iostream>

// Keep the compiler happy.
Database DB;

class VBufAllocatorTest : public testing::Test {

protected:
	VersionBufferAllocator *alloc;
	void *drained;

	virtual void SetUp() {
		ASSERT_TRUE(sizeof(CompositeKey) == 20);
		alloc = new 
			VersionBufferAllocator(100*VersionBufferAllocator::BUFFER_SIZE);
		ASSERT_TRUE(alloc != NULL);
		drained = NULL;
	}

	// Removes all buffers from the free-list to simulate an empty allocator.
	virtual void DrainAllocator() {
		drained = alloc->freeList;
		alloc->freeList = NULL;
	}
	
	// Returns all drained buffers to the allocator.
	virtual void UndrainAllocator() {
		alloc->freeList = drained;
		drained = NULL;
	}

	virtual int GetOffset(VersionBuffer *vbuffer) {
		return vbuffer->offset;
	}
};

TEST_F(VBufAllocatorTest, VBufTest) {
	VersionBuffer buf(alloc);
	CompositeKey def;
	bool success = false;
	
	// Append a single element to the VersionBuffer.
	success = buf.Append(def);
	ASSERT_TRUE(success);
	
	// Don't allow more blocks to be allocted.
	DrainAllocator();

	// Five more should succeed. A total of six at this point.
	for (int i = 0; i < 5; ++i) {
		success = buf.Append(def);
		ASSERT_TRUE(success);
	}

	// The seventh should fail.
	success = buf.Append(def);
	ASSERT_FALSE(success);

	// All of these should fail.
	for (int i = 0; i < 1000; ++i) {
		success = buf.Append(def);
		ASSERT_FALSE(success);
	}

	// Return the buffer.
	alloc->ReturnBuffers(&buf);
	
	// At this point, we should allow six more appends.
	for (int i = 0; i < 6; ++i) {
		success = buf.Append(def);
		ASSERT_TRUE(success);
	}
	
	// Any further should fail.
	for (int i = 0; i < 1000; ++i) {
		success = buf.Append(def);
		ASSERT_FALSE(success);
		ASSERT_EQ(5, GetOffset(&buf));
	}
	
	// Get back the remaining blocks.
	UndrainAllocator();

	// Try to allocate all other blocks.
	for (int i = 0; i < 99; ++i) {
		for (int j = 0; j < 6; ++j) {
			success = buf.Append(def);
			ASSERT_TRUE(success);
		}
	}
	
	// Done with all blocks. This shouldn't succeed.
	success = buf.Append(def);
	ASSERT_FALSE(success);

	// Any further should fail.
	for (int i = 0; i < 1000; ++i) {
		success = buf.Append(def);
		ASSERT_FALSE(success);
		ASSERT_EQ(5, GetOffset(&buf));
	}
	
	// Try to allocate from another VersionBuffer.
	// This should also fail.
	VersionBuffer bufOther(alloc);
	success = bufOther.Append(def);
	ASSERT_FALSE(success);	

	// Any further should fail.
	for (int i = 0; i < 1000; ++i) {
		success = bufOther.Append(def);
		ASSERT_FALSE(success);
	}

	// Return the buffers.
	alloc->ReturnBuffers(&buf);

	// Split the allocator's blocks among two buffers.
	for (int i = 0; i < 100; ++i) {
		
		VersionBuffer *bufPtr;
		if (i % 2 == 0) {
			bufPtr = &buf;
		}
		else {
			bufPtr = &bufOther;
		}

		for (int j = 0; j < 6; ++j) {
			success = bufPtr->Append(def);
			ASSERT_TRUE(success);
		}
		ASSERT_EQ(5, GetOffset(bufPtr));
	}

	success = bufOther.Append(def);
	ASSERT_FALSE(success);
	
	success = buf.Append(def);
	ASSERT_FALSE(success);		
}

/*
 * Test getting and returning buffers.
 */
TEST_F(VBufAllocatorTest, Get) {
	void *successfulBuf = NULL, *failBuf = NULL;
	bool success = alloc->GetBuffer(&successfulBuf);

	ASSERT_EQ(true, success);
	ASSERT_TRUE(successfulBuf != NULL);
	
	memset(successfulBuf, 0x00, VersionBufferAllocator::BUFFER_SIZE);
	VersionBuffer temp(alloc);
	temp.AddLink(successfulBuf);

	// Get rid of all the buffers. Ensure that we're unable to allocate any more
	// buffers.
	DrainAllocator();
	success = alloc->GetBuffer(&failBuf);
	ASSERT_EQ(false, success);
	ASSERT_TRUE(failBuf == NULL);

	// Return the buffer. Ensure we can allocate just one.
	alloc->ReturnBuffers(&temp);
	successfulBuf = NULL;
	
	// Should succeed.
	success = alloc->GetBuffer(&successfulBuf);
	ASSERT_EQ(true, success);
	ASSERT_TRUE(successfulBuf != NULL);
	temp.AddLink(successfulBuf);

	// Should fail.
	success = alloc->GetBuffer(&failBuf);
	ASSERT_EQ(false, success);
	ASSERT_TRUE(failBuf == NULL);

	UndrainAllocator();
	success = alloc->GetBuffer(&successfulBuf);
	ASSERT_EQ(true, success);
	ASSERT_TRUE(successfulBuf != NULL);
	temp.AddLink(successfulBuf);
	
	success = alloc->GetBuffer(&successfulBuf);
	ASSERT_EQ(true, success);
	ASSERT_TRUE(successfulBuf != NULL);
	temp.AddLink(successfulBuf);
	
	alloc->ReturnBuffers(&temp);	
}

int main(int argc, char **argv) {
	testing::InitGoogleTest(&argc, argv);
	return RUN_ALL_TESTS();
}
