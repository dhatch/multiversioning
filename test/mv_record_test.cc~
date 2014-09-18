#include <gtest/gtest.h>

#include <mv_record.h>
#include <mv_table.h>
#include <database.h>

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

class MVTablePartitionTest : public MVAllocatorTest {

protected:
	MVTablePartition *partition;

	virtual void SetUp() {
		MVAllocatorTest::SetUp();
		partition = new MVTablePartition(100, allocator);
	}
};

class MVTableTest : public MVTablePartitionTest {
	
};


TEST_F(MVTablePartitionTest, PartitionTest) {
	CompositeKey toAdd;
	bool success = false;
	uint64_t version;

	for (int i = 0; i < 100; ++i) {
		toAdd.key = (uint64_t)i;
		success = partition->GetLatestVersion(toAdd, &version);
		ASSERT_FALSE(success);
	}
	
	version = 1;
	for (int i = 0; i < 100; ++i) {
		toAdd.key = (uint64_t)i;
		success = partition->WriteNewVersion(toAdd, NULL, version);
		ASSERT_TRUE(success);
	}

	version = 0;
	for (int i = 0; i < 100; ++i) {
		toAdd.key = (uint64_t)i;
		success = partition->GetLatestVersion(toAdd, &version);
		ASSERT_TRUE(success);
		ASSERT_EQ(1, version);
	}

	version = 5;
	for (int i = 0; i < 100; ++i) {
		if (i % 2 == 0) {
			toAdd.key = (uint64_t)i;
			success = partition->WriteNewVersion(toAdd, NULL, version);
			ASSERT_TRUE(success);
		}
	}

	for (int i = 0; i < 100; ++i) {
		toAdd.key = (uint64_t)i;
		success = partition->GetLatestVersion(toAdd, &version);
		ASSERT_TRUE(success);
		
		if (i % 2 == 0) {
			ASSERT_EQ(5, version);
		}
		else {
			ASSERT_EQ(1, version);
		}
	}
}

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

class CatalogTest : public testing::Test {

protected:
	Catalog catalog;
	Database database;
	
	virtual void StartUp() { }
};

TEST_F(CatalogTest, DBTest) {
	MVTable *tbl[100];
	bool success = false;

	for (uint32_t i = 0; i < 100; ++i) {
		tbl[i] = (MVTable*)(uint64_t)i;
	}

	for (uint32_t i = 0; i < 100; ++i) {
		if (i % 2 == 0) {
			success = database.PutTable(i, tbl[i]);
			ASSERT_TRUE(success);
		}
		else {
			success = database.PutTable(i-1, tbl[i]);
			ASSERT_FALSE(success);
		}
	}

	for (uint32_t i = 0; i < 100; ++i) {
		MVTable *cmp = NULL;
		success = database.GetTable(i, &cmp);
		
		if (i % 2 == 0) {
			ASSERT_TRUE(success);
			ASSERT_EQ(tbl[i], cmp);
		}
		else {
			ASSERT_FALSE(success);
			ASSERT_EQ(NULL, cmp);
		}
	}
}

TEST_F(CatalogTest, CatalogTest) {
	MVTable *tbl[100];
	bool success = false;

	for (uint32_t i = 0; i < 100; ++i) {
		tbl[i] = (MVTable*)(uint64_t)i;
	}

	for (uint32_t i = 0; i < 100; ++i) {
		if (i % 2 == 0) {
			success = catalog.PutTable(i, tbl[i]);
			ASSERT_TRUE(success);
		}
		else {
			success = catalog.PutTable(i-1, tbl[i]);
			ASSERT_FALSE(success);
		}
	}

	for (uint32_t i = 0; i < 100; ++i) {
		MVTable *cmp = NULL;
		success = catalog.GetTable(i, &cmp);
		
		if (i % 2 == 0) {
			ASSERT_TRUE(success);
			ASSERT_EQ(tbl[i], cmp);
		}
		else {
			ASSERT_FALSE(success);
			ASSERT_EQ(NULL, cmp);
		}
	}
}

TEST(CompositeKeyTest, HashTest) {
	CompositeKey keys[1000];
	uint64_t hashes[1000];
	for (int i = 0; i < 1000; ++i) {
		keys[i].tableId = 0;
		keys[i].key = (uint64_t)(i % 100);
		keys[i].version = (uint64_t)i;		
		hashes[i] = CompositeKey::Hash(&keys[i]);
	}
	
	for (int i = 0; i < 100; ++i) {
		for (int j = 100; j < 1000; j += 100) {
			ASSERT_EQ(hashes[i], hashes[i+j]);
		}
	}	
}
