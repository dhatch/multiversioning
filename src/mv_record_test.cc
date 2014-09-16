#include <gtest/gtest.h>

#include <mv_record.h>

class MVAllocatorTest : public testing::Test {

protected:

	MVRecordAllocator *allocator;
	MVRecord *drained;

	virtual void SetUp() {
		allocator = new MVRecordAllocator(sizeof(MVRecord)*1000);
	}

	virtual void DrainAllocator() {
		drained = allocator->freeList;
		allocator->freeList = NULL;
	}

	virtual void UndrainAllocator() {
		allocator->freeList = drained;
		drained = NULL;
	}
};

TEST_F(MVAllocatorTest, AllocTest) {
	
	MVRecord *cur0 = NULL, *cur1 = NULL;
	bool success = false;
	
	// Allocate two records.
	success = allocator->GetRecord(&cur0);	
	ASSERT_TRUE(success);
	ASSERT_TRUE(cur0 != NULL);
	
	success = allocator->GetRecord(&cur1);
	ASSERT_TRUE(success);
	ASSERT_TRUE(cur1 != NULL);
	
	DrainAllocator();
	
	// This allocation should fail.
	MVRecord *temp = NULL;
	success = allocator->GetRecord(&temp);
	ASSERT_TRUE(temp == NULL);
	ASSERT_FALSE(success);

	MVRecordList ret0 = {.head = cur0, .tail = cur0};
	allocator->ReturnMVRecords(ret0);

	cur0 = NULL;

	success = allocator->GetRecord(&cur0);
	ASSERT_TRUE(success);
	ASSERT_TRUE(cur0 != NULL);
	
	cur1->recordLink = cur0;
	
	success = allocator->GetRecord(&cur0);
	ASSERT_FALSE(success);
	ASSERT_TRUE(cur0 == NULL);
}
