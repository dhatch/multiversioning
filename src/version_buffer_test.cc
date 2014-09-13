#include <database.h>
#include <preprocessor.h>
#include <gtest/gtest.h>

Database DB;

class VBufAllocatorTest : public testing::Test {

protected:
	VersionBufferAllocator *alloc;
	void *drained;

	virtual void SetUp() {
		alloc = new 
			VersionBufferAllocator(10*VersionBufferAllocator::BUFFER_SIZE);
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
};

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
