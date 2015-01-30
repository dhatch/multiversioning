#include <gtest/gtest.h>
#include <preprocessor.h>
#include <database.h>

#include <cstdlib>
#include <unordered_map>
#include <time.h>

using namespace std;
Database DB(2);

class SchedulerTest : public testing::Test {
        
protected:
        
        MVScheduler *sched;
        int numRecords;
        SimpleQueue<ActionBatch> *leaderInputQueue;
        SimpleQueue<ActionBatch> *leaderOutputQueue;

        unordered_map<uint64_t, uint64_t> versionTracker;
        unordered_map<uint64_t, Action*> actionTracker;

        virtual void SetUp()
        {          
    
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

                size_t *partitionSizes = (size_t*)malloc(sizeof(size_t)*2);
                partitionSizes[0] = 1<<20;
                partitionSizes[1] = 1<<20;

                // Create the scheduler state. Use a single thread.
                MVSchedulerConfig config = {
                        0, 
                        0,
                        (1 << 26),
                        2,
                        partitionSizes,
                        1,
                        1,
                        0,
                        leaderInputQueue,
                        leaderOutputQueue,
                        NULL,
                        NULL,
                        NULL,
                };
                sched = new (config.cpuNumber) MVScheduler(config);
                sched->txnCounter = 1;
        }

        static uint64_t GetUniqueKey(std::set<uint64_t> &previousKeys)
        {
                while (true) {
                        uint64_t key = (uint64_t)(rand() % 100);
                        if (previousKeys.find(key) == previousKeys.end()) {
                                previousKeys.insert(key);
                        
                                return key;
                        }
                }
        }


public:
        /*
         * Setup a batch of transactions.
         */
        static Action **setup_input(uint32_t num_txns, uint32_t txn_sz)
        {
                Action **ret = (Action**)malloc(sizeof(Action*)*num_txns);
                for (uint32_t i = 0; i < num_txns; ++i) {
                        ret[i] = setup_single_txn(txn_sz);
                        ret[i]->version = i;
                }
                return ret;
        }

        /*
         * Setup a single txn.
         */
        static Action *setup_single_txn(uint32_t txn_sz)
        {
                CompositeKey tmp;
                std::set<uint64_t> used_keys;
                Action *ret = new Action();
                for (uint32_t i = 0; i < txn_sz; ++i) {
                        uint64_t key = GetUniqueKey(used_keys);
                        tmp.tableId = rand() % 2;
                        tmp.key = key;
                        tmp.threadId = 0;
                        
                        ret->readset.push_back(tmp);
                        ret->writeset.push_back(tmp);
                }
                ret->combinedHash = 1;
                return ret;
        }

        void run_scheduler(Action **txns, uint32_t num_txns)
        {
                ActionBatch batch = {txns, num_txns};
                leaderInputQueue->EnqueueBlocking(batch);
                sched->Run();
                leaderOutputQueue->DequeueBlocking();
        }

        static SimpleQueue<ActionBatch> *setup_queue(uint64_t queue_sz)
        {
                char *array = (char*)alloc_mem(CACHE_LINE*queue_sz, 1);
                memset(array, 0, CACHE_LINE*queue_sz);
                return new SimpleQueue<ActionBatch>(array, queue_sz);
        }

        void verify_tables(Action **txns, uint32_t num_txns, uint32_t txn_sz)
        {
                MVTablePartition *tbl0, *tbl1;
                tbl0 = sched->partitions[0];
                tbl1 = sched->partitions[1];
                ASSERT_TRUE(tbl0 != NULL && tbl1 != NULL);
                for (uint32_t i = 0; i < num_txns; ++i) {
                        uint64_t version = txns[i]->version;
                        for (uint32_t j = 0; j < txn_sz; ++j) {
                                CompositeKey key = txns[i]->writeset[j];

                                MVTablePartition *tbl;
                                switch (txns[i]->writeset[j].tableId) {
                                case 0:
                                        tbl = tbl0;
                                        break;
                                case 1:
                                        tbl = tbl1;
                                        break;
                                default:
                                        ASSERT_TRUE(false);
                                }
                                MVRecord *record =
                                        tbl->GetMVRecord(key, version);
                                ASSERT_TRUE(record->value == NULL);
                                ASSERT_EQ(txns[i], record->writer);
                        }
                }
        }
};


TEST_F(SchedulerTest, Test)
{
        uint32_t num_txns = 1000;
        uint32_t txn_sz = 10;
        Action **txns = setup_input(num_txns, txn_sz);
        run_scheduler(txns, num_txns);
        verify_tables(txns, num_txns, txn_sz);

}

int main(int argc, char **argv)
{
        srand(time(NULL));
        testing::InitGoogleTest(&argc, argv);
        return RUN_ALL_TESTS();
}
