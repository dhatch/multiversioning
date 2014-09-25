#include <database.h>
#include <preprocessor.h>
#include <cpuinfo.h>

#include <gperftools/profiler.h>

#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>

Database DB;

int NumProcs;

timespec diff_time(timespec end, timespec start) {
    timespec temp;
    if ((end.tv_nsec - start.tv_nsec) < 0) {
        temp.tv_sec = end.tv_sec - start.tv_sec - 1;
        temp.tv_nsec = 1000000000+end.tv_nsec-start.tv_nsec;
    }
    else {
        temp.tv_sec = end.tv_sec-start.tv_sec;
        temp.tv_nsec = end.tv_nsec-start.tv_nsec;
    }
    return temp;
}


MVSchedulerConfig SetupLeaderSched(int cpuNumber, int numSchedThreads) {
    // Set up queues for coordinating with other threads in the system.
    char *inputArray = (char*)alloc_mem(CACHE_LINE*256, 0);            
    SimpleQueue<ActionBatch> *leaderInputQueue = new SimpleQueue<ActionBatch>(inputArray, 256);
    char *outputArray = (char*)alloc_mem(CACHE_LINE*256, 0);
    SimpleQueue<ActionBatch> *leaderOutputQueue = new SimpleQueue<ActionBatch>(outputArray, 256);
    
    // Queues to coordinate with subordinate scheduler threads.
    SimpleQueue<ActionBatch> **leaderEpochStartQueues = 
        (SimpleQueue<ActionBatch>**)alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1, 0);
    SimpleQueue<ActionBatch> **leaderEpochStopQueues = 
        (SimpleQueue<ActionBatch>**)alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1, 0);    

    // We need a queue of size 2 because each the leader and subordinates run epochs in 
    // lock step.
    for (int i = 0; i < numSchedThreads; ++i) {        
        char *startArray = (char*)alloc_mem(CACHE_LINE*2, 0);
        SimpleQueue<ActionBatch> *startQueue = new SimpleQueue<ActionBatch>(startArray, 2);
        char *stopArray = (char*)alloc_mem(CACHE_LINE*2, 0);
        SimpleQueue<ActionBatch> *stopQueue = new SimpleQueue<ActionBatch>(stopArray, 2);
        
        assert(startQueue != NULL);
        assert(stopQueue != NULL);

        leaderEpochStartQueues[i] = startQueue;
        leaderEpochStopQueues[i] = stopQueue;
    }
    
    MVSchedulerConfig config {
        cpuNumber,						// core to bind
        0,								// threadId
            0,
        leaderInputQueue,
        leaderOutputQueue,
        leaderEpochStartQueues,
        leaderEpochStopQueues,
        NULL,
        NULL,        
    };
    
    return config;
}

MVSchedulerConfig SetupSubordinateSched(int cpuNumber, uint32_t threadId, 
                                        MVSchedulerConfig leaderConfig) {
    MVSchedulerConfig config {
        cpuNumber,
        threadId,
            0,
        NULL,
        NULL,
        NULL,
        NULL,
        leaderConfig.leaderEpochStartQueues[threadId-1],
        leaderConfig.leaderEpochStopQueues[threadId-1],
    };
    assert(config.subordInputQueue != NULL && config.subordOutputQueue != NULL);
    return config;
}

MVScheduler** SetupSchedulers(int numProcs, SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                              SimpleQueue<ActionBatch> **outputQueueRef_OUT) {
    MVScheduler **schedArray = (MVScheduler**)malloc(sizeof(MVScheduler*)*numProcs);
    MVSchedulerConfig leaderConfig = SetupLeaderSched(10,  // cpuNumber
                                                      numProcs);
    schedArray[0] = new MVScheduler(leaderConfig);
    for (int i = 1; i < numProcs; ++i) {
        MVSchedulerConfig subordConfig = SetupSubordinateSched(i+10, i, leaderConfig);
        schedArray[i] = new MVScheduler(subordConfig);
    }
    
    *inputQueueRef_OUT = leaderConfig.leaderInputQueue;
    *outputQueueRef_OUT = leaderConfig.leaderOutputQueue;
    return schedArray;
}

void SetupDatabase(int numProcs, uint64_t allocatorSize, uint64_t tableSize) {
    MVTablePartition **partArray = (MVTablePartition**)malloc(sizeof(MVTable*)*numProcs);
    
    //    uint64_t allocatorChunkSize = allocatorSize/numProcs;
    uint64_t tableChunkSize = tableSize/numProcs;

    for (int i = 0; i < numProcs; ++i) {
        MVRecordAllocator *recordAlloc = 
            new MVRecordAllocator(allocatorSize, i+10);

        MVTablePartition *part = new MVTablePartition(tableChunkSize, i+10, recordAlloc);
        partArray[i] = part;
    }

    MVTable *tbl = new MVTable(numProcs, partArray);    
    bool success = DB.PutTable(0, tbl);
    assert(success);
}

bool SortCmp(CompositeKey key1, CompositeKey key2) {
    uint32_t thread1 = CompositeKey::Hash(&key1) % MVScheduler::NUM_CC_THREADS;
    uint32_t thread2 = CompositeKey::Hash(&key2) % MVScheduler::NUM_CC_THREADS;
    return thread1 > thread2;
}

ActionBatch CreateRandomAction(int txnSize, uint32_t epochSize, int numRecords) {
    std::cout << MVScheduler::NUM_CC_THREADS << "\n";
    Action **ret = new Action*[epochSize];    
    assert(ret != NULL);
    std::set<uint64_t> seenKeys;
    for (uint32_t j = 0; j < epochSize; ++j) {
        seenKeys.clear();
        ret[j] = new Action();
        assert(ret[j] != NULL);
        ret[j]->combinedHash = 0;
        for (int i = 0; i < txnSize; ++i) {        
            while (true) {
                uint64_t key = (uint64_t)(rand() % numRecords);
                if (seenKeys.find(key) == seenKeys.end()) {
                    seenKeys.insert(key);
                    CompositeKey toAdd(0, key);
                    uint32_t threadId = CompositeKey::Hash(&toAdd) % MVScheduler::NUM_CC_THREADS;
                    toAdd.threadId = threadId;
                    ret[j]->readset.push_back(toAdd);
                    ret[j]->writeset.push_back(toAdd);
                    break;
                }
            }
        }
        
        //        std::sort(ret[j]->readset.begin(), ret[j]->readset.end(), SortCmp);
        //        std::sort(ret[j]->writeset.begin(), ret[j]->writeset.end(), SortCmp);
        //        int counts[MVScheduler::NUM_CC_THREADS];
        //        memset(counts, 0, sizeof(counts));
        for (int k = 0; k < txnSize; ++k) {
            uint32_t threadId = CompositeKey::Hash(&ret[j]->readset[k]) % MVScheduler::NUM_CC_THREADS;
            ret[j]->combinedHash |= (1<<threadId);
            //            counts[threadId] += 1;
        }
        
        /*
        int prevCount = 0;
        for (int k = 0; k < MVScheduler::NUM_CC_THREADS; ++k) {
            Range curRange;
            curRange.start = prevCount;
            curRange.end = prevCount + counts[k];
            prevCount += counts[k];
            ret[j]->readRange.push_back(curRange);
            ret[j]->writeRange.push_back(curRange);
        }
        */
    }
    
    ActionBatch batch = {
        ret,
        epochSize,
    };
    return batch;

}

void SetupInput(SimpleQueue<ActionBatch> *input, int numEpochs, int epochSize, int numRecords, int txnsize) {
    for (int i = 0; i < numEpochs; ++i) {
        ActionBatch curBatch = CreateRandomAction(txnsize, epochSize, numRecords);
        input->EnqueueBlocking(curBatch);        
    }
}

void DoExperiment(int numProcs, int numRecords, int epochSize, int numEpochs, int txnSize) {
    int successPin = pin_thread(79);
    if (successPin != 0) {
        assert(false);
    }

    MVScheduler **schedThreads = NULL;
    SimpleQueue<ActionBatch> *inputQueue = NULL;
    SimpleQueue<ActionBatch> *outputQueue = NULL;

    // Set up the database.
    std::cout << "Start setup database...\n";
    SetupDatabase(numProcs, 1<<30, numRecords);
    std::cout << "Setup database...\n";
    
    // Set up the scheduler threads.
    schedThreads = SetupSchedulers(numProcs, &inputQueue, &outputQueue);
    assert(schedThreads != NULL);
    assert(inputQueue != NULL);
    assert(outputQueue != NULL);
    std::cout << "Setup scheduler threads...\n";

    // Set up the input.
    SetupInput(inputQueue, numEpochs, epochSize, numRecords, txnSize);
    std::cout << "Setup input...\n";

    // Run the experiment.
    ProfilerStart("/home/jmf/multiversioning/db.prof");
    std::cout << "Running experiment. Epochs: " << numEpochs << "\n";
    timespec start_time, end_time;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
    for (int i = 0; i < numProcs; ++i) {
        schedThreads[i]->Run();
    }
    for (int i = 0; i < numEpochs; ++i) {
        outputQueue->DequeueBlocking();
        //        std::cout << "Iteration " << i << "\n";
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    ProfilerStop();
    timespec elapsed_time = diff_time(end_time, start_time);
    double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
    std::cout << elapsedMilli << '\n';
    std::ofstream resultFile;
    resultFile.open("results.txt", std::ios::app | std::ios::out);
    resultFile << elapsedMilli << " " << numEpochs*epochSize << " " << numProcs << "\n";
    //    std::cout << "Time elapsed: " << elapsedMilli << "\n";
    resultFile.close();
}

// arg0: number of scheduler threads
// arg1: number of records in the database
// arg2: number of txns in an epoch
// arg3: number of epochs
int main(int argc, char **argv) {
    assert(argc == 6);
    int numProcs = atoi(argv[1]);
    int numRecords = atoi(argv[2]);
    int epochSize = atoi(argv[3]);
    int numEpochs = atoi(argv[4]);
    int txnSize = atoi(argv[5]);

    MVScheduler::NUM_CC_THREADS = (uint32_t)numProcs;
    DoExperiment(numProcs, numRecords, epochSize, numEpochs, txnSize);
    exit(0);
}

