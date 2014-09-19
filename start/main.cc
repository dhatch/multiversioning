#include <database.h>
#include <preprocessor.h>
#include <cpuinfo.h>

#include <set>
#include <iostream>

Database DB;

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
    char *inputArray = (char*)malloc(CACHE_LINE*1000);            
    SimpleQueue<ActionBatch> *leaderInputQueue = new SimpleQueue<ActionBatch>(inputArray, 1000);
    char *outputArray = (char*)malloc(CACHE_LINE*1000);
    SimpleQueue<ActionBatch> *leaderOutputQueue = new SimpleQueue<ActionBatch>(outputArray, 1000);
    
    // Queues to coordinate with subordinate scheduler threads.
    SimpleQueue<ActionBatch> **leaderEpochStartQueues = 
        (SimpleQueue<ActionBatch>**)malloc(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1);
    SimpleQueue<ActionBatch> **leaderEpochStopQueues = 
        (SimpleQueue<ActionBatch>**)malloc(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1);    

    // We need a queue of size 2 because each the leader and subordinates run epochs in 
    // lock step.
    for (int i = 0; i < numSchedThreads-1; ++i) {        
        char *startArray = (char*)malloc(CACHE_LINE*2);
        SimpleQueue<ActionBatch> *startQueue = new SimpleQueue<ActionBatch>(startArray, 2);
        char *stopArray = (char*)malloc(CACHE_LINE*2);
        SimpleQueue<ActionBatch> *stopQueue = new SimpleQueue<ActionBatch>(stopArray, 2);
        
        leaderEpochStartQueues[i] = startQueue;
        leaderEpochStopQueues[i] = stopQueue;
    }
    
    MVSchedulerConfig config = {
        cpuNumber,						// core to bind
        0,								// threadId
        (1<<25),						// bufSize
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
    MVSchedulerConfig config = {
        cpuNumber,
        threadId,
        (1 << 25),
        NULL,
        NULL,
        NULL,
        NULL,
        leaderConfig.leaderEpochStartQueues[threadId],
        leaderConfig.leaderEpochStopQueues[threadId],
    };
    return config;
}

MVScheduler** SetupSchedulers(int numProcs, SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                              SimpleQueue<ActionBatch> **outputQueueRef_OUT) {
    MVScheduler **schedArray = (MVScheduler**)malloc(sizeof(MVScheduler*)*numProcs);
    MVSchedulerConfig leaderConfig = SetupLeaderSched(1,  // cpuNumber
                                                      numProcs);
    schedArray[0] = new MVScheduler(leaderConfig);
    for (int i = 1; i < numProcs; ++i) {
        MVSchedulerConfig subordConfig = SetupSubordinateSched(1+i, i, leaderConfig);
        schedArray[i] = new MVScheduler(subordConfig);
    }
    
    *inputQueueRef_OUT = leaderConfig.leaderInputQueue;
    *outputQueueRef_OUT = leaderConfig.leaderOutputQueue;
    return schedArray;
}

void SetupDatabase(int numProcs, uint64_t allocatorSize, uint64_t tableSize, uint64_t numRecords) {
    MVTablePartition **partArray = (MVTablePartition**)malloc(sizeof(MVTable*)*numProcs);
    
    uint64_t allocatorChunkSize = allocatorSize/numProcs;
    uint64_t tableChunkSize = tableSize/numProcs;

    for (int i = 0; i < numProcs; ++i) {
        MVRecordAllocator *recordAlloc = 
            new MVRecordAllocator(allocatorChunkSize*sizeof(MVRecord));

        MVTablePartition *part = new MVTablePartition(tableChunkSize, recordAlloc);
        partArray[i] = part;
    }

    CompositeKey temp;
    temp.tableId = 0;
    for (uint64_t i = 0; i < numRecords; ++i) {
        temp.key = i;
        uint64_t partitionId = (uint64_t)(CompositeKey::Hash(&temp) % numProcs);
        bool success = partArray[partitionId]->WriteNewVersion(temp, NULL, 0);        
        assert(success);
    }

    MVTable *tbl = new MVTable(numProcs, partArray);    
    bool success = DB.PutTable(0, tbl);
    assert(success);
}

ActionBatch CreateRandomAction(int txnSize, uint32_t epochSize, int numRecords) {
    Action **ret = new Action*[epochSize];    
    std::set<uint64_t> seenKeys;
    for (uint32_t j = 0; j < epochSize; ++j) {
        seenKeys.clear();
        ret[j] = new Action();
        for (int i = 0; i < txnSize; ++i) {        
            while (true) {
                uint64_t key = (uint64_t)(rand() % numRecords);
                if (seenKeys.find(key) == seenKeys.end()) {
                    seenKeys.insert(key);
                    CompositeKey toAdd(0, key);
                    ret[j]->readset.push_back(toAdd);
                    ret[j]->writeset.push_back(toAdd);
                    break;
                }
            }
        }
    }
    
    ActionBatch batch = {
        ret,
        epochSize,
    };
    return batch;

}

void SetupInput(SimpleQueue<ActionBatch> *input, int numEpochs, int epochSize, int numRecords) {
    for (int i = 0; i < numEpochs; ++i) {
        ActionBatch curBatch = CreateRandomAction(20, epochSize, numRecords);
        input->EnqueueBlocking(curBatch);        
    }
}

void DoExperiment(int numProcs, int numRecords, int epochSize, int numEpochs) {
    int successPin = pin_thread(0);
    if (successPin != 0) {
        assert(false);
    }

    MVScheduler **schedThreads = NULL;
    SimpleQueue<ActionBatch> *inputQueue = NULL;
    SimpleQueue<ActionBatch> *outputQueue = NULL;

    // Set up the database.
    SetupDatabase(numProcs, 1<<30, 2*numRecords, numRecords);
    
    // Set up the scheduler threads.
    schedThreads = SetupSchedulers(numProcs, &inputQueue, &outputQueue);
    assert(schedThreads != NULL);
    assert(inputQueue != NULL);
    assert(outputQueue != NULL);

    // Set up the input.
    SetupInput(inputQueue, numEpochs, epochSize, numRecords);

    // Run the experiment.
    timespec start_time, end_time;
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
    for (int i = 0; i < epochSize; ++i) {
        outputQueue->DequeueBlocking();
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    timespec elapsed_time = diff_time(end_time, start_time);
    double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;

    std::cout << "Time elapsed: " << elapsedMilli;
}

// arg0: number of scheduler threads
// arg1: number of records in the database
// arg2: number of txns in an epoch
// arg3: number of epochs
int main(int argc, char **argv) {
    assert(argc == 5);
    int numProcs = atoi(argv[1]);
    int numRecords = atoi(argv[2]);
    int epochSize = atoi(argv[3]);
    int numEpochs = atoi(argv[4]);
    
    DoExperiment(numProcs, numRecords, epochSize, numEpochs);
}
