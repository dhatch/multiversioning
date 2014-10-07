#include <gtest/gtest.h>
#include <preprocessor.h>
#include <database.h>

#include <cstdlib>
#include <unordered_map>
#include <time.h>

using namespace std;
Database DB;

class SchedulerTest : public testing::Test {
        
protected:
        
  MVScheduler *sched;
  int numRecords;
  SimpleQueue<ActionBatch> *leaderInputQueue;
  SimpleQueue<ActionBatch> *leaderOutputQueue;

  unordered_map<uint64_t, uint64_t> versionTracker;
  unordered_map<uint64_t, Action*> actionTracker;

  virtual void SetUp() {          
    
    // Create a table with few records so that we can test that dependencies 
    // are maintained properly.
    numRecords = 100;

    // Allocate input/output queues for concurrency control stage.
    uint64_t queueSize = 256;
    char *inputArray = (char*)alloc_mem(CACHE_LINE*queueSize, 1);
    char *outputArray = (char*)alloc_mem(CACHE_LINE*queueSize, 1);
    memset(inputArray, 0, CACHE_LINE*queueSize);
    memset(outputArray, 0, CACHE_LINE*queueSize);
    leaderInputQueue = new SimpleQueue<ActionBatch>(inputArray, queueSize);
    leaderOutputQueue = new SimpleQueue<ActionBatch>(outputArray, queueSize);

    size_t *partitionSizes = (size_t*)malloc(sizeof(size_t));
    partitionSizes[0] = 1<<20;

    // Create the scheduler state. Use a single thread.
    MVSchedulerConfig config = {
      0, 
      0,
      (1 << 28),
      1,
      partitionSizes,
      leaderInputQueue,
      leaderOutputQueue,
      NULL,
      NULL,
      NULL,
      NULL,
    };
    sched = new MVScheduler(config);
    sched->txnCounter = 1;
  }
};

uint64_t GetUniqueKey(std::set<uint64_t> &previousKeys) {
  while (true) {
    uint64_t key = (uint64_t)(rand() % 100);
    if (previousKeys.find(key) == previousKeys.end()) {
      previousKeys.insert(key);
      return key;
    }
  }
}

TEST_F(SchedulerTest, Test) {
  Action **toSchedule = (Action**)malloc(1000*sizeof(Action*));
  int writesetSize = 10;
  srand(time(NULL));    

  // Setup the input transactions. 
  CompositeKey temp;
  std::set<uint64_t> previousKeys;      // Track keys used by a particular txn
  for (int i = 0; i < 1000; ++i) {
    previousKeys.clear();
    
    Action *toAdd = new Action();
    for (int j = 0; j < writesetSize; ++j) {
      
      // Generate a unique key for each write and add the record to the 
      // read/write set.
      uint64_t key = GetUniqueKey(previousKeys);
      temp.tableId = 0;
      temp.key = key;
      temp.threadId = 0;

      toAdd->readset.push_back(temp);
      toAdd->writeset.push_back(temp);
    }
    
    // Set the transaction's bitmask so that it's not ignored by the scheduler.
    toAdd->combinedHash = 1;
    toAdd->version = (uint64_t)i;
    toSchedule[i] = toAdd;
  }

  // Kick off the scheduler.
  ActionBatch batch = {toSchedule, 1000};
  leaderInputQueue->EnqueueBlocking(batch);
  sched->Run();
  leaderOutputQueue->DequeueBlocking();
  
  // Verify that transactions have been correctly scheduled.
  MVTable *tbl;
  bool success = DB.GetTable(0, &tbl);
  ASSERT_TRUE(success);
  for (int i = 0; i < 1000; ++i) {
    for (int j = 0; j < writesetSize; ++j) {
      Record rec;
      success = tbl->GetVersion(0, toSchedule[i]->writeset[j], 
                                toSchedule[i]->version, &rec);
      ASSERT_FALSE(rec.isMaterialized);
      ASSERT_EQ(&toSchedule[i], rec.rec);
    }           
  }
}

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
