#include <lock_manager.h>
#include <gtest/gtest.h>
#include <time.h>
#include <set>
#include <cstring>

//uint64_t SchedulerTest::GetUniqueKey(std::set<uint64_t> &previousKeys);

class LockManagerTest : public testing::Test {
protected:
        virtual void SetUp() {
                srand(time(NULL));
        }
};

/*
TEST_F(LockManagerTest, TestLock) {
        LockManager *manager = new LockManager(10, 0);
        LockingAction **txnArray = 
                (LockingAction**)malloc(sizeof(LockingAction*)*1000);
        memset(txnArray, 0x0, sizeof(LockingAction*)*1000);
        int writesetSize = 5;

        std::set<uint64_t> previousKeys;  
        LockingCompositeKey compKey;
        compKey.tableId = 0;
        for (int i = 0; i < 1000; ++i) {
                previousKeys.clear();    
                LockingAction *toAdd = new LockingAction();
                for (int j = 0; j < writesetSize; ++j) {
                uint64_t key = SchedulerTest::GetUniqueKey(previousKeys);
                        compKey.key = key;
                        toAdd->writeset.push_back(compKey);
                }
                manager->AcquireLocks(toAdd);
                txnArray[i] = toAdd;
        }
}

TEST_F(LockManagerTest, TestBucket) {
        LockBucket bucket;
  
        LockBucketEntry *entries = 
                (LockBucketEntry*)malloc(sizeof(LockBucketEntry)*100);

        for (int i = 0; i < 100; ++i) {
                bucket.AppendEntry(&entries[i]);
        }
  
        int i = 0;
        volatile LockBucketEntry *temp = bucket.head;
        while (temp != NULL) {
                ASSERT_TRUE(i < 100);
                ASSERT_TRUE(temp == &entries[i]);
                temp = temp->next;
                i += 1;
        }
}
*/
