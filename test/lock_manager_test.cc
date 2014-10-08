#include <lock_manager.h>
#include <gtest/gtest.h>

using namespace std;

class LockManagerTest : public testing::Test {
  

};

TEST_F(LockManagerTest, TestLock) {
  LockManager *manager = new LockManager(10, 0);
  
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

