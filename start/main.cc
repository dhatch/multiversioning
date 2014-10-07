#include <database.h>
#include <preprocessor.h>
#include <cpuinfo.h>

#include <gperftools/profiler.h>

#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>

#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

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


MVSchedulerConfig SetupLeaderSched(int cpuNumber, int numSchedThreads, 
                                   size_t alloc, size_t *partSizes) {
  // Set up queues for coordinating with other threads in the system.
  char *inputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);            
  SimpleQueue<ActionBatch> *leaderInputQueue = 
    new SimpleQueue<ActionBatch>(inputArray, INPUT_SIZE);
  char *outputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);
  SimpleQueue<ActionBatch> *leaderOutputQueue = 
    new SimpleQueue<ActionBatch>(outputArray, INPUT_SIZE);

  std::cout << "Setup input & output threads.\n";
    
  // Queues to coordinate with subordinate scheduler threads.
  SimpleQueue<ActionBatch> **leaderEpochStartQueues = 
    (SimpleQueue<ActionBatch>**)alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1, 
                                          OFFSET_CORE(0));
  SimpleQueue<ActionBatch> **leaderEpochStopQueues = 
    (SimpleQueue<ActionBatch>**)alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*numSchedThreads-1, 
                                          OFFSET_CORE(0));    
  std::cout << "Setup epoch threads for coordination.\n";
  
  // We need a queue of size 2 because each the leader and subordinates run epochs in 
  // lock step.
  char *startArray = (char*)alloc_mem(CACHE_LINE*4*numSchedThreads, 
                                      OFFSET_CORE(0));
  char *stopArray = &startArray[CACHE_LINE*2*numSchedThreads];
  for (int i = 0; i < numSchedThreads; ++i) {        
    auto startQueue = 
      new SimpleQueue<ActionBatch>(&startArray[2*CACHE_LINE*i], 2);
    auto stopQueue = 
      new SimpleQueue<ActionBatch>(&stopArray[2*CACHE_LINE*i], 2);
        
    assert(startQueue != NULL);
    assert(stopQueue != NULL);

    leaderEpochStartQueues[i] = startQueue;
    leaderEpochStopQueues[i] = stopQueue;
  }
    
  MVSchedulerConfig config {
    cpuNumber,                       // cpuNumber
      0,                             // threadId
      alloc,                         // allocatorSize
      1,                             // number of tables
      partSizes,                     // partition sizes
      leaderInputQueue,              // leaderInputQueue
      leaderOutputQueue,             // leaderOutputQueue
      leaderEpochStartQueues,        // leaderEpochStartQueue
      leaderEpochStopQueues,         // leaderEpochStopQueues
      NULL,                          // subordInputQueue
      NULL,                          // subordOutputQueue
      };
    
  return config;
}

MVSchedulerConfig SetupSubordinateSched(int cpuNumber, 
                                        uint32_t threadId, 
                                        MVSchedulerConfig leaderConfig, 
                                        size_t alloc, 
                                        size_t *partSizes) {
  MVSchedulerConfig config {
    cpuNumber,                                          // cpuNumber
      threadId,                                         // threadId
      alloc,                                            // allocatorSize
      1,                                                // number of tables
      partSizes,                                        // partitionSize
      NULL,                                             // leaderInputQueue
      NULL,                                             // leaderOutputQueue
      NULL,                                             // leaderEpochStartQueue
      NULL,                                             // leaderEpochStopQueue
      leaderConfig.leaderEpochStartQueues[threadId-1],  // subordInputQueue
      leaderConfig.leaderEpochStopQueues[threadId-1],   // subordOutputQueue
      };

    assert(config.subordInputQueue != NULL && config.subordOutputQueue != NULL);
    return config;
}

MVScheduler** SetupSchedulers(int numProcs, 
                              SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                              SimpleQueue<ActionBatch> **outputQueueRef_OUT, 
                              size_t allocatorSize, 
                              size_t tableSize) {
  
  size_t partitionChunk = tableSize/numProcs;
  size_t *tblPartitionSizes = (size_t*)malloc(sizeof(size_t));
  tblPartitionSizes[0] = partitionChunk;

    MVScheduler **schedArray = 
      (MVScheduler**)alloc_mem(sizeof(MVScheduler*)*numProcs, 79);
    MVSchedulerConfig leaderConfig = 
      SetupLeaderSched(OFFSET_CORE(0),  // cpuNumber
                       numProcs, allocatorSize, tblPartitionSizes);

    schedArray[0] = new MVScheduler(leaderConfig);

    for (int i = 1; i < numProcs; ++i) {
      MVSchedulerConfig subordConfig = 
        SetupSubordinateSched(OFFSET_CORE(i), i, leaderConfig, allocatorSize, 
                              tblPartitionSizes);
        schedArray[i] = new MVScheduler(subordConfig);
    }
    
    *inputQueueRef_OUT = leaderConfig.leaderInputQueue;
    *outputQueueRef_OUT = leaderConfig.leaderOutputQueue;
    return schedArray;
}

/*
void SetupDatabase(int numProcs, uint64_t allocatorSize, uint64_t tableSize) {
    MVTablePartition **partArray = (MVTablePartition**)alloc_mem(sizeof(MVTable*)*numProcs, 0);
    memset(partArray, 0x00, sizeof(MVTable*)*numProcs);

    //    uint64_t allocatorChunkSize = allocatorSize/numProcs;
    uint64_t tableChunkSize = tableSize/numProcs;

    for (int i = 0; i < numProcs; ++i) {
        MVRecordAllocator *recordAlloc = 
          new MVRecordAllocator(allocatorSize, OFFSET_CORE(i));

        MVTablePartition *part = new MVTablePartition(tableChunkSize, OFFSET_CORE(i), recordAlloc);
        partArray[i] = part;
    }

    MVTable *tbl = new MVTable(numProcs, partArray);    
    bool success = DB.PutTable(0, tbl);
    assert(success);
}
*/

bool SortCmp(CompositeKey key1, CompositeKey key2) {
    uint32_t thread1 = CompositeKey::Hash(&key1) % MVScheduler::NUM_CC_THREADS;
    uint32_t thread2 = CompositeKey::Hash(&key2) % MVScheduler::NUM_CC_THREADS;
    return thread1 > thread2;
}

ActionBatch CreateRandomAction(int txnSize, uint32_t epochSize, int numRecords) {
  Action **ret = (Action**)alloc_mem(sizeof(Action*)*epochSize, 79);
    assert(ret != NULL);
    std::set<uint64_t> seenKeys;
    uint64_t counter = 0;
    for (uint32_t j = 0; j < epochSize; ++j) {
        seenKeys.clear();
        ret[j] = new Action();
        assert(ret[j] != NULL);
        ret[j]->combinedHash = 0;
        for (int i = 0; i < txnSize; ++i) {        
            /*
            CompositeKey toAdd(0, counter);
            uint32_t threadId = counter % MVScheduler::NUM_CC_THREADS;
            toAdd.threadId = threadId;
            ret[j]->readset.push_back(toAdd);
            ret[j]->writeset.push_back(toAdd);
            ret[j]->combinedHash |= (((uint64_t)1)<<threadId);
            counter += 1;
            */
            while (true) {
                uint64_t key = (uint64_t)(rand() % numRecords);
                //                uint64_t counterMod = counter % MVScheduler::NUM_CC_THREADS;
                //                uint64_t keyMod = key % MVScheduler::NUM_CC_THREADS;

                //                uint32_t threadId = CompositeKey::Hash(&toAdd) % MVScheduler::NUM_CC_THREADS;

                if (seenKeys.find(key) == seenKeys.end()) {
                    seenKeys.insert(key);
                    CompositeKey toAdd(0, key);
                    uint32_t threadId = CompositeKey::HashKey(&toAdd) % MVScheduler::NUM_CC_THREADS;
                    toAdd.threadId = threadId;
                    ret[j]->readset.push_back(toAdd);
                    ret[j]->writeset.push_back(toAdd);
                    ret[j]->combinedHash |= (((uint64_t)1)<<threadId);
                    break;
                }
            }
            counter += 1;
        }
    }
    
    ActionBatch batch = {
        ret,
        epochSize,
    };
    return batch;

}

void SetupInput(SimpleQueue<ActionBatch> *input, int numEpochs, int epochSize, int numRecords, int txnsize) {
    for (int i = 0; i < numEpochs+1; ++i) {
        ActionBatch curBatch = CreateRandomAction(txnsize, epochSize, numRecords);
        input->EnqueueBlocking(curBatch);        
    }
}

void DoExperiment(int numProcs, int numRecords, int epochSize, int numEpochs, int txnSize) {
    MVScheduler **schedThreads = NULL;
    SimpleQueue<ActionBatch> *inputQueue = NULL;
    SimpleQueue<ActionBatch> *outputQueue = NULL;


    // Set up the database.
    //    std::cout << "Start setup database...\n";
    //    SetupDatabase(numProcs, (uint64_t)1<<30, numRecords);
    //    std::cout << "Setup database...\n";
    
    // Set up the scheduler threads.
    schedThreads = SetupSchedulers(numProcs, &inputQueue, &outputQueue, 
                                   (uint64_t)1<<30, numRecords);
    assert(schedThreads != NULL);
    assert(inputQueue != NULL);
    assert(outputQueue != NULL);
    std::cout << "Setup scheduler threads...\n";
    std::cout << "Num CC Threads: " << MVScheduler::NUM_CC_THREADS << "\n";
    
    int successPin = pin_thread(79);
    if (successPin != 0) {
        assert(false);
    }

    // Set up the input.
    SetupInput(inputQueue, numEpochs, epochSize, numRecords, txnSize);
    std::cout << "Setup input...\n";


    // Run the experiment.
    for (int i = 0; i < numProcs; ++i) {
        schedThreads[i]->Run();
    }


    std::cout << "Running experiment. Epochs: " << numEpochs << "\n";
    timespec start_time, end_time;

    outputQueue->DequeueBlocking();
    ProfilerStart("/home/jmf/multiversioning/db.prof");
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);

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

void DoHashes(int numProcs, int numRecords, int epochSize, int numEpochs, 
              int txnSize) {
  char *inputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);            
  SimpleQueue<ActionBatch> *inputQueue = 
    new SimpleQueue<ActionBatch>(inputArray, INPUT_SIZE);
  char *outputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);
  SimpleQueue<ActionBatch> *outputQueue = 
    new SimpleQueue<ActionBatch>(outputArray, INPUT_SIZE);
  SetupInput(inputQueue, numEpochs, epochSize, numRecords, txnSize);  

  auto hasher = new (0) MVActionHasher(0, inputQueue, outputQueue);

  int successPin = pin_thread(79);
  if (successPin != 0) {
    assert(false);
  }

  timespec start_time, end_time;
  hasher->Run();
  outputQueue->DequeueBlocking();
  ProfilerStart("/home/jmf/multiversioning/db.prof");
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
  for (int i = 0; i < numEpochs; ++i) {
    outputQueue->DequeueBlocking();
  }
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
  ProfilerStop();
  timespec elapsed_time = diff_time(end_time, start_time);
  double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
  std::cout << elapsedMilli << '\n';
  std::ofstream resultFile;
  resultFile.open("hashing.txt", std::ios::app | std::ios::out);
  resultFile << elapsedMilli << " " << numEpochs*epochSize << " " << numProcs << "\n";
  //    std::cout << "Time elapsed: " << elapsedMilli << "\n";
  resultFile.close();
}

// arg0: number of scheduler threads
// arg1: number of records in the database
// arg2: number of txns in an epoch
// arg3: number of epochs
int main(int argc, char **argv) {
    assert(argc == 7);
    int numProcs = atoi(argv[1]);
    int numRecords = atoi(argv[2]);
    int epochSize = atoi(argv[3]);
    int numEpochs = atoi(argv[4]);
    int txnSize = atoi(argv[5]);
    int exptId = atoi(argv[6]);

    srand(time(NULL));
    MVScheduler::NUM_CC_THREADS = (uint32_t)numProcs;
    if (exptId == 0) {
      DoExperiment(numProcs, numRecords, epochSize, numEpochs, txnSize);
    }
    else {
      DoHashes(numProcs, numRecords, epochSize, numEpochs, txnSize);
    }
    exit(0);
}
