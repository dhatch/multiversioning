#include <gtest/gtest.h>

#include <mv_record.h>
#include <mv_table.h>
#include <database.h>
#include <cpuinfo.h>

class MVAllocatorTest : public testing::Test {

protected:

        MVRecordAllocator *allocator;
        MVRecord *drained;

        virtual void SetUp() {
                allocator = new (0) MVRecordAllocator(sizeof(MVRecord)*1000, 0);
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
                partition = new MVTablePartition(100, 0, allocator);
        }
};

class MVTableTest : public MVTablePartitionTest {
        
};


TEST_F(MVTablePartitionTest, PartitionTest) {
        CompositeKey toAdd;
        bool success = false;
        uint64_t version;

        // Create a bunch of dummy transactions to insert into the partition.
        Action **actionPtrs = (Action**)malloc(sizeof(Action*)*200);
        for (int i = 0; i < 200; ++i) {
                actionPtrs[i] = new Action();
        }

        for (int i = 0; i < 100; ++i) {
                toAdd.key = (uint64_t)i;
                success = partition->GetLatestVersion(toAdd, &version);
                ASSERT_FALSE(success);
        }
        
        version = 1;
        for (int i = 0; i < 100; ++i) {
                toAdd.key = (uint64_t)i;
                ASSERT_TRUE(actionPtrs[i] != NULL);
                success = partition->WriteNewVersion(toAdd, actionPtrs[i], version);
                ASSERT_TRUE(success);
        }

        Record rec;
        for (int i = 0; i < 100; ++i) {
                toAdd.key = (uint64_t)i;
    
                // Version at time 1 should exist.
                success = partition->GetVersion(toAdd, 1, &rec);
                ASSERT_TRUE(success);
                ASSERT_FALSE(rec.isMaterialized);
                ASSERT_EQ(actionPtrs[i], rec.rec);

                // Version at time 0 should not exist.
                success = partition->GetVersion(toAdd, 0, &rec);
                ASSERT_FALSE(success);
        }
  
        version = 5;
        for (int i = 0; i < 100; ++i) {
                toAdd.key = (uint64_t)i;
                success = partition->WriteNewVersion(toAdd, actionPtrs[i+100], version);
                ASSERT_TRUE(success);
        }

        for (int i = 0; i < 100; ++i) {
                toAdd.key = (uint64_t)i;
                success = partition->GetVersion(toAdd, 1, &rec);
                ASSERT_TRUE(success);
                ASSERT_FALSE(rec.isMaterialized);
                ASSERT_EQ(actionPtrs[i], rec.rec);
    
                // Version at time 4 should be the same as time 1.
                success = partition->GetVersion(toAdd, 4, &rec);
                ASSERT_TRUE(success);
                ASSERT_FALSE(rec.isMaterialized);
                ASSERT_EQ(actionPtrs[i], rec.rec);
    
                // Check that version at 5 is properly inserted.
                success = partition->GetVersion(toAdd, 5, &rec);
                ASSERT_TRUE(success);
                ASSERT_FALSE(rec.isMaterialized);
                ASSERT_EQ(actionPtrs[i+100], rec.rec);
    
                // Version at time 100 should be the same as time 5.
                success = partition->GetVersion(toAdd, 100, &rec);
                ASSERT_TRUE(success);
                ASSERT_FALSE(rec.isMaterialized);
                ASSERT_EQ(actionPtrs[i+100], rec.rec);

                // Version at time 0 should not exist.
                success = partition->GetVersion(toAdd, 0, &rec);
                ASSERT_FALSE(success);
        }
}

/*
  TEST_F(MVAllocatorTest, TestNUMAAllocation) {
  // Allocate an enormous amount of memory larger than the total amount on
  // the numa node.
  void *temp0 = alloc_mem((uint64_t)1 << 33, 0);
  ASSERT_TRUE(temp0 != NULL);
  numa_free(temp0, (uint64_t)1<<33);

  void *temp1 = alloc_mem(((uint64_t)1 << 35), 0);
  ASSERT_TRUE(temp1 == NULL);
  }
*/

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

        MVRecordList ret0 = {cur0, cur0};
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
                keys[i].threadId = (uint32_t)i;         
                hashes[i] = CompositeKey::HashKey(&keys[i]);
        }
        
        for (int i = 0; i < 100; ++i) {
                for (int j = 100; j < 1000; j += 100) {
                        ASSERT_EQ(hashes[i], hashes[i+j]);
                }
        }       
}
