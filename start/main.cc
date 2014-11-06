#include <database.h>
#include <preprocessor.h>
#include <cpuinfo.h>
#include <config.h>
#include <eager_worker.h>
#include <executor.h>

#include <gperftools/profiler.h>

#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>

#define RECYCLE_QUEUE_SIZE 64
#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

Database DB(1);

// uint64_t dbSize = ((uint64_t)1<<36);

int NumProcs;
uint32_t numLockingRecords;
uint64_t recordSize;

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

void CreateQueues(int cpuNumber, uint32_t subCount, 
                  SimpleQueue<ActionBatch>*** OUT_PUB_QUEUES,
                  SimpleQueue<ActionBatch>*** OUT_SUB_QUEUES) {
  if (subCount == 0) {
    *OUT_PUB_QUEUES = NULL;
    *OUT_SUB_QUEUES = NULL;
    return;
  }
  // Allocate space to keep queues for subordinates
  void *pubTemp = alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*subCount, 
                            cpuNumber);
  void *subTemp = alloc_mem(sizeof(SimpleQueue<ActionBatch>*)*subCount,
                            cpuNumber);
  auto pubQueues = (SimpleQueue<ActionBatch>**)pubTemp;
  auto subQueues = (SimpleQueue<ActionBatch>**)subTemp;

  // Allocate space for queue data
  char *pubArray = (char*)alloc_mem(CACHE_LINE*4*subCount, cpuNumber);
  assert(pubArray != NULL);
  memset(pubArray, 0x00, CACHE_LINE*4*subCount);
  char *subArray = &pubArray[CACHE_LINE*2*subCount];
      
  // Allocate space for queue meta-data
  auto pubMetaData = 
    (SimpleQueue<ActionBatch>*)alloc_mem(sizeof(SimpleQueue<ActionBatch>)*2*subCount, 
                                         cpuNumber);
  assert(pubMetaData != NULL);
  memset(pubMetaData, 0x0, sizeof(SimpleQueue<ActionBatch>)*2*subCount);
  auto subMetaData = &pubMetaData[subCount];

  // Initialize meta-data with SimpleQueue constructor
  for (uint32_t i = 0; i < subCount; ++i) {
    auto pubQueue = 
      new (&pubMetaData[i]) 
      SimpleQueue<ActionBatch>(&pubArray[2*CACHE_LINE*i], 2);
    auto subQueue =
      new (&subMetaData[i])
      SimpleQueue<ActionBatch>(&subArray[2*CACHE_LINE*i], 2);
    assert(pubQueue != NULL && subQueue != NULL);
    pubQueues[i] = pubQueue;
    subQueues[i] = subQueue;
  }
  
  *OUT_PUB_QUEUES = pubQueues;
  *OUT_SUB_QUEUES = subQueues;
}

MVSchedulerConfig SetupSched(int cpuNumber, 
                             int threadId, 
                             int numThreads, 
                             size_t alloc, 
                             size_t *partSizes, 
                             uint32_t numRecycles,
                             SimpleQueue<ActionBatch> *inputQueue,
                             uint32_t numOutputs,
                             SimpleQueue<ActionBatch> *outputQueues) {
  assert(inputQueue != NULL && outputQueues != NULL);
  uint32_t subCount;
  SimpleQueue<ActionBatch> **pubQueues, **subQueues;
  if (cpuNumber % 10 == 0) {
    if (cpuNumber == 0) {
      
      // Figure out how many subordinates this thread is in charge of.
      uint32_t localSubordinates = numThreads > 10? 9 : numThreads-1;
      uint32_t numRemoteSockets = 
        numThreads/10 + (numThreads % 10 == 0? 0 : 1) - 1;
      subCount = (uint32_t)(localSubordinates + numRemoteSockets);
      
      CreateQueues(cpuNumber, subCount, &pubQueues, &subQueues);
    }
    else {
      int myDiv = cpuNumber/10;
      int totalDiv = numThreads/10;
      if (myDiv < totalDiv) {
        subCount = 9;
      }
      else {
        subCount = (uint32_t)(numThreads - cpuNumber - 1);
      }      

      CreateQueues(cpuNumber, subCount, &pubQueues, &subQueues);      
    }
  }
  else {
    subCount = 0;
    pubQueues = NULL;
    subQueues = NULL;
  }

  // Create recycle queue
  uint32_t recycleQueueSize = CACHE_LINE*64*numRecycles;
  uint32_t queueArraySize = numRecycles*sizeof(SimpleQueue<MVRecordList>*);
  uint32_t queueMetadataSize = numRecycles*sizeof(SimpleQueue<MVRecordList>);
  uint32_t blobSize = recycleQueueSize + queueArraySize + queueMetadataSize;

  // Allocate a blob of memory for all recycle queue related data
  void *blob = alloc_mem(blobSize, cpuNumber);
  SimpleQueue<MVRecordList> **queueArray = (SimpleQueue<MVRecordList>**)blob;
  SimpleQueue<MVRecordList> *queueMetadata = 
    (SimpleQueue<MVRecordList>*)((char*)blob + queueArraySize);
  char *queueData = (char*)blob + queueArraySize + queueMetadataSize;  

  for (uint32_t i = 0; i < numRecycles; ++i) {
    uint32_t offset = i*CACHE_LINE*RECYCLE_QUEUE_SIZE;
    queueArray[i] = 
      new (&queueMetadata[i]) 
      SimpleQueue<MVRecordList>(queueData+offset, RECYCLE_QUEUE_SIZE);
  }

  MVSchedulerConfig cfg = {
    cpuNumber,
    threadId,
    alloc,
    1,
    partSizes,
    numOutputs,
    subCount,
    numRecycles,
    inputQueue,
    outputQueues,
    pubQueues,
    subQueues,
    queueArray,
  };
  return cfg;
}


/*
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
*/

GarbageBinConfig SetupGCConfig(uint32_t numCCThreads, uint32_t numWorkerThreads,
                               uint32_t numTables,
                               int cpuNumber,
                               volatile uint32_t *GClowWaterMarkPtr) {
  assert(GClowWaterMarkPtr != NULL);

  // First initialize garbage collection meta data. We need space to keep 
  // references to remote threads's GC queues.
  uint32_t concControlGC_sz = numCCThreads*sizeof(SimpleQueue<MVRecordList>*);
  uint32_t workerGC_sz = 
    numWorkerThreads*numTables*sizeof(SimpleQueue<RecordList>*);
  uint32_t gc_sz = concControlGC_sz + workerGC_sz;
  void *blob = alloc_mem(gc_sz, cpuNumber);
  memset(blob, 0x0, gc_sz);
  
  SimpleQueue<MVRecordList> **ccGCQueues = (SimpleQueue<MVRecordList>**)blob;
  SimpleQueue<RecordList> **workerGCQueues = 
    (SimpleQueue<RecordList>**)((char*)blob + concControlGC_sz);

  GarbageBinConfig gcConfig = {
    numCCThreads,
    numWorkerThreads,
    numTables,
    cpuNumber,
    GClowWaterMarkPtr,
    ccGCQueues,
    workerGCQueues,
  };
  return gcConfig;
}

// Setup GC queues for a single worker. Other worker threads will insert 
// recycled data into these queues.
SimpleQueue<RecordList>* SetupGCQueues(uint32_t cpuNumber, 
                                       uint32_t queuesPerTable, 
                                       uint32_t numTables) {
  // Allocate a blob of data to hold queue meta data (head & tail ptrs), and
  // actual queue entries.
  uint32_t metaDataSz = 
    sizeof(SimpleQueue<RecordList>)*queuesPerTable*numTables;
  uint32_t dataSz = CACHE_LINE*RECYCLE_QUEUE_SIZE*numTables*queuesPerTable;
  uint32_t totalSz = metaDataSz + dataSz;
  void *blob = alloc_mem(totalSz, cpuNumber);
  memset(blob, 0x00, totalSz);

  // The first part of the blob corresponds to queue space, the second is queue 
  // entry space
  SimpleQueue<RecordList> *queueData = (SimpleQueue<RecordList>*)blob;
  char *data = (char*)blob + metaDataSz;

  // Use for computing appropriate offsets
  uint32_t qSz = sizeof(SimpleQueue<RecordList>);
  uint32_t singleDataSz = CACHE_LINE*RECYCLE_QUEUE_SIZE;
  
  // Initialize queues
  for (uint32_t i = 0; i < numTables; ++i) {
    for (uint32_t j = 0; j < queuesPerTable; ++j) {
      
      uint32_t queueOffset = (i*queuesPerTable + j);
      uint32_t dataOffset = queueOffset*singleDataSz;

      SimpleQueue<RecordList> *temp = new (&queueData[queueOffset]) 
        SimpleQueue<RecordList>(data + dataOffset, RECYCLE_QUEUE_SIZE);        
    }
  }
  return queueData;
}

ExecutorConfig SetupExec(uint32_t cpuNumber, uint32_t threadId, 
                         uint32_t numWorkerThreads, 
                         volatile uint32_t *epoch, 
                         volatile uint32_t *GClowWaterMarkPtr,
                         uint64_t *recordSizes, 
                         uint64_t *allocSizes,
                         SimpleQueue<ActionBatch> *inputQueue, 
                         SimpleQueue<ActionBatch> *outputQueue,
                         uint32_t numCCThreads,
                         uint32_t numTables, 
                         uint32_t queuesPerTable) {  
  assert(inputQueue != NULL);  
  
  // GC config
  GarbageBinConfig gcConfig = SetupGCConfig(numCCThreads, numWorkerThreads, 
                                            numTables, 
                                            cpuNumber,
                                            GClowWaterMarkPtr);

  // GC queues for this particular worker
  SimpleQueue<RecordList> *gcQueues = SetupGCQueues(cpuNumber, queuesPerTable, 
                                                    numTables);
  ExecutorConfig config = {
    threadId,
    numWorkerThreads,
    cpuNumber,
    epoch,
    GClowWaterMarkPtr,
    inputQueue,
    outputQueue,
    1,
    recordSizes,
    allocSizes,
    1,
    gcQueues,
    gcConfig,
  };
  return config;
}

Executor** SetupExecutors(uint32_t cpuStart,
                          uint32_t numWorkers, 
                          uint32_t numCCThreads,
                          uint32_t queuesPerTable,
                          SimpleQueue<ActionBatch> *inputQueue,
                          SimpleQueue<ActionBatch> *outputQueue,
                          uint32_t queuesPerCCThread,
                          SimpleQueue<MVRecordList> ***ccQueues) {  
  assert(queuesPerCCThread == numWorkers);
  assert(queuesPerTable == numWorkers);

  uint64_t threadDbSz = (1<<30); //dbSize / numWorkers;
  Executor **execs = (Executor**)malloc(sizeof(Executor*)*numWorkers);
  volatile uint32_t *epochArray = 
    (volatile uint32_t*)malloc(sizeof(uint32_t)*(numWorkers+1));  
  memset((void*)epochArray, 0x0, sizeof(uint32_t)*(numWorkers+1));

  uint32_t numTables = 1;

  uint64_t *sizeData = (uint64_t*)malloc(sizeof(uint32_t)*2);
  sizeData[0] = recordSize;
  sizeData[1] = threadDbSz/recordSize;

  // First pass, create configs. Each config contains a reference to each 
  // worker's local GC queue.
  ExecutorConfig configs[numWorkers];  
  for (uint32_t i = 0; i < numWorkers; ++i) {
    SimpleQueue<ActionBatch> *curOutput = NULL;
    if (i == 0) {
      curOutput = outputQueue;
    }
    configs[i] = SetupExec(cpuStart+i, i, numWorkers, &epochArray[i], 
                           &epochArray[numWorkers],
                           &sizeData[0],
                           &sizeData[1],
                           &inputQueue[i],
                           curOutput,
                           numCCThreads,
                           1,
                           queuesPerTable);
  }
  
  // Second pass, connect recycled data producers with consumers
  for (uint32_t i = 0; i < numWorkers; ++i) {
    
    // Connect to cc threads
    for (uint32_t j = 0; j < numCCThreads; ++j) {
      configs[i].garbageConfig.ccChannels[j] = ccQueues[j][i%queuesPerCCThread];
      assert(configs[i].garbageConfig.ccChannels[j] != NULL);
    }

    // Connect to every workers gc queue
    for (uint32_t j = 0; j < numWorkers; ++j) {
      for (uint32_t k = 0; k < numTables; ++k) {
        configs[i].garbageConfig.workerChannels[j] = 
          &configs[j].recycleQueues[k*queuesPerTable+(i%queuesPerTable)];
      }
      assert(configs[i].garbageConfig.workerChannels[j] != NULL);
      execs[i] = new ((int)(cpuStart+i)) Executor(configs[i]);
    }
  }
  return execs;
}

// Setup an array of queues.
template<class T>
SimpleQueue<T>* SetupQueuesMany(uint32_t numEntries, uint32_t numQueues, int cpu) {
  size_t metaDataSz = sizeof(SimpleQueue<T>)*numQueues; // SimpleQueue structs
  size_t queueDataSz = CACHE_LINE*numEntries*numQueues; // queue data
  size_t totalSz = metaDataSz+queueDataSz;
  void *data = alloc_mem(totalSz, cpu);
  memset(data, 0x00, totalSz);

  char *queueData = (char*)data + metaDataSz;
  size_t dataDelta = CACHE_LINE*numEntries;
  SimpleQueue<T> *queues = (SimpleQueue<T>*)data;

  // Initialize queue structs
  for (uint32_t i = 0; i < numQueues; ++i) {
    SimpleQueue<T> *temp = 
      new (&queues[i]) SimpleQueue<T>(&queueData[dataDelta*i], numEntries);
  }
  return queues;
}

MVScheduler** SetupSchedulers(int numProcs, 
                              SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                              SimpleQueue<ActionBatch> **outputQueueRefs_OUT, 
                              uint32_t numOutputs,
                              size_t allocatorSize, 
                              size_t tableSize, 
                              SimpleQueue<MVRecordList> ***gcRefs_OUT) {  
  size_t partitionChunk = tableSize/numProcs;
  size_t *tblPartitionSizes = (size_t*)malloc(sizeof(size_t));
  tblPartitionSizes[0] = partitionChunk;

  // Set up queues for leader thread
  char *inputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 0);            
  SimpleQueue<ActionBatch> *leaderInputQueue = 
    new SimpleQueue<ActionBatch>(inputArray, INPUT_SIZE);

  SimpleQueue<ActionBatch> *leaderOutputQueues = 
    SetupQueuesMany<ActionBatch>(INPUT_SIZE, (uint32_t)numOutputs, 0);
  
  MVScheduler **schedArray = 
    (MVScheduler**)alloc_mem(sizeof(MVScheduler*)*numProcs, 79);
  
  MVSchedulerConfig globalLeaderConfig = SetupSched(0, 0, numProcs, 
                                                    allocatorSize,
                                                    tblPartitionSizes, 
                                                    numOutputs,
                                                    leaderInputQueue,
                                                    numOutputs,
                                                    leaderOutputQueues);

  schedArray[0] = 
    new (globalLeaderConfig.cpuNumber) MVScheduler(globalLeaderConfig);
  gcRefs_OUT[0] = globalLeaderConfig.recycleQueues;

  MVSchedulerConfig localLeaderConfig = globalLeaderConfig;
  
  for (int i = 1; i < numProcs; ++i) {
    if (i % 10 == 0) {
      int leaderNum = i/10;
      auto inputQueue = globalLeaderConfig.pubQueues[9+leaderNum-1];
      auto outputQueue = globalLeaderConfig.subQueues[9+leaderNum-1];
      MVSchedulerConfig config = SetupSched(i, i, numProcs, allocatorSize, 
                                            tblPartitionSizes, 
                                            numOutputs,
                                            inputQueue, 
                                            1,
                                            outputQueue);
      schedArray[i] = new (config.cpuNumber) MVScheduler(config);
      gcRefs_OUT[i] = config.recycleQueues;
      localLeaderConfig = config;
    }
    else {
      int index = i%10;      
      auto inputQueue = localLeaderConfig.pubQueues[index-1];
      auto outputQueue = localLeaderConfig.subQueues[index-1];
      MVSchedulerConfig subConfig = SetupSched(i, i, numProcs, allocatorSize, 
                                               tblPartitionSizes, 
                                               numOutputs,
                                               inputQueue, 
                                               1,
                                               outputQueue);
      schedArray[i] = new (subConfig.cpuNumber) MVScheduler(subConfig);
      gcRefs_OUT[i] = subConfig.recycleQueues;
    }
  }
  
  *inputQueueRef_OUT = leaderInputQueue;
  *outputQueueRefs_OUT = leaderOutputQueues;
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


ActionBatch CreateRandomAction(int txnSize, uint32_t epochSize, int numRecords, 
                               uint32_t epoch, uint32_t experiment) {
  Action **ret = (Action**)alloc_mem(sizeof(Action*)*epochSize, 79);
    assert(ret != NULL);
    std::set<uint64_t> seenKeys;
    uint64_t counter = 0;
    for (uint32_t j = 0; j < epochSize; ++j) {
        seenKeys.clear();
        ret[j] = new Action();// new RMWAction();
        assert(ret[j] != NULL);
        ret[j]->combinedHash = 0;
        ret[j]->version = (((uint64_t)epoch<<32) | counter);
        ret[j]->state = STICKY;
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
                    if (experiment == 0) {
                      ret[j]->writeset.push_back(toAdd);
                    }
                    else if (experiment == 1 && j < 2) {
                      ret[j]->writeset.push_back(toAdd);
                    }
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

void SetupInputArray(std::vector<ActionBatch> *input, int numEpochs, int epochSize, 
                     int numRecords, int txnsize, uint32_t experiment) {
  for (int i = 0; i < numEpochs; ++i) {
    ActionBatch curBatch = CreateRandomAction(txnsize, epochSize, numRecords, 
                                              (uint32_t)(i+2), experiment);
    input->push_back(curBatch);
  }
}

ActionBatch LoadDatabaseTxns(int numRecords, int txnSize) {
  int numTxns = numRecords/txnSize + (numRecords%txnSize > 0? 1 : 0);
  Action **txns = (Action**)malloc(sizeof(InsertAction*)*numTxns);
  uint64_t counter = 0;
  for (int i = 0; i < numTxns; ++i) {    
    // Create a new txn
    InsertAction *curAction = new InsertAction();
    assert(curAction != NULL);
    curAction->state = STICKY;
    curAction->combinedHash = 0;
    curAction->version = (((uint64_t)1<<32) | counter);

    // Generate the txn's write set
    for (uint64_t j = 0;
         (j<(uint64_t)txnSize) && (counter<(uint64_t)numRecords);
         ++j, ++counter) {
      
      CompositeKey toAdd(0, counter);
      uint32_t threadId = 
        CompositeKey::HashKey(&toAdd) % MVScheduler::NUM_CC_THREADS;
      toAdd.threadId = threadId;
      curAction->writeset.push_back(toAdd);
      curAction->combinedHash |= (((uint64_t)1)<<threadId);
    }
    txns[i] = curAction;
  }
  
  ActionBatch ret = {
    txns,
    (uint32_t)numTxns,
  };
  return ret;
}

void SetupInput(SimpleQueue<ActionBatch> *input, int numEpochs, int epochSize, 
                int numRecords, int txnsize) {
    for (int i = 0; i < numEpochs+1; ++i) {
      ActionBatch curBatch = CreateRandomAction(txnsize, epochSize, numRecords, (uint32_t)i, 0);
      //      input->push_back(curBatch);
      input->EnqueueBlocking(curBatch);        
    }
}



void DoExperiment(int numCCThreads, int numExecutors, int numRecords, 
                  int epochSize,
                  int numEpochs, 
                  int txnSize,
                  uint32_t experiment) {
    MVScheduler **schedThreads = NULL;
    SimpleQueue<ActionBatch> *schedInputQueue = NULL;
    SimpleQueue<ActionBatch> *schedOutputQueues = NULL;
    SimpleQueue<MVRecordList> **schedGCQueues[numCCThreads];

    SimpleQueue<ActionBatch> *outputQueue = SetupQueuesMany<ActionBatch>(INPUT_SIZE, 1, 71);

    // Set up the scheduler threads.
    schedThreads = SetupSchedulers(numCCThreads, &schedInputQueue, 
                                   &schedOutputQueues, 
                                   (uint32_t)numExecutors,
                                   (uint64_t)1<<28, 
                                   numRecords, 
                                   schedGCQueues);
    assert(schedThreads != NULL);
    assert(schedInputQueue != NULL);
    assert(schedOutputQueues != NULL);
    std::cout << "Setup scheduler threads...\n";
    std::cout << "Num CC Threads: " << MVScheduler::NUM_CC_THREADS << "\n";
    
    int successPin = pin_thread(79);
    if (successPin != 0) {
        assert(false);
    }

    // Set up the input.
    ActionBatch loadInput = LoadDatabaseTxns(numRecords, 100);
    std::vector<ActionBatch> inputPlaceholder;
    SetupInputArray(&inputPlaceholder, numEpochs, epochSize, numRecords, txnSize, experiment);
    std::cout << "Setup input...\n";
    
    // Setup executors
    Executor **execThreads = SetupExecutors(numCCThreads, 
                                            numExecutors, 
                                            numCCThreads,
                                            numExecutors,
                                            schedOutputQueues,
                                            outputQueue,
                                            numExecutors,
                                            schedGCQueues);
    
    schedInputQueue->EnqueueBlocking(loadInput);
    
    // Run the experiment.
    for (int i = 0; i < numCCThreads; ++i) {
        schedThreads[i]->Run();
    }

    for (int i = 0; i < numExecutors; ++i) {
      execThreads[i]->Run();
    }

    for (int i = 0; i < numEpochs; ++i) {
      schedInputQueue->EnqueueBlocking(inputPlaceholder[i]);
    }

    outputQueue->DequeueBlocking();

    std::cout << "Running experiment. Epochs: " << numEpochs << "\n";
    timespec start_time, end_time;

    //ProfilerStart("/home/jmf/multiversioning/db.prof");
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);

    for (int i = 0; i < numEpochs-50; ++i) {
        outputQueue->DequeueBlocking();
        //        std::cout << "Iteration " << i << "\n";
    }
    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
    //    ProfilerStop();

    timespec elapsed_time = diff_time(end_time, start_time);
    double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
    std::cout << elapsedMilli << '\n';
    std::ofstream resultFile;
    resultFile.open("results.txt", std::ios::app | std::ios::out);
    resultFile << elapsedMilli << " " << (numEpochs-50)*epochSize << " " << numExecutors << "\n";
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
  //  ProfilerStart("/home/jmf/multiversioning/db.prof");
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
  for (int i = 0; i < numEpochs; ++i) {
    outputQueue->DequeueBlocking();
  }
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
  //  ProfilerStop();
  timespec elapsed_time = diff_time(end_time, start_time);
  double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
  std::cout << elapsedMilli << '\n';
  std::ofstream resultFile;
  resultFile.open("hashing.txt", std::ios::app | std::ios::out);
  resultFile << elapsedMilli << " " << numEpochs*epochSize << " " << numProcs << "\n";
  //    std::cout << "Time elapsed: " << elapsedMilli << "\n";
  resultFile.close();
}

/*
bool SortCmp(LockingCompositeKey key1, LockingCompositeKey key2) {
    uint64_t hash1 = LockingCompositeKey::Hash(&key1) % numLockingRecords;
    uint64_t hash2 = LockingCompositeKey::Hash(&key2) % numLockingRecords;
    return hash1 > hash2;
    //    return (key1.tableId > key2.tableId) || (key1.key > key2.key);
    //    uint32_t thread1 = CompositeKey::Hash(&key1) % MVScheduler::NUM_CC_THREADS;
    //    uint32_t thread2 = CompositeKey::Hash(&key2) % MVScheduler::NUM_CC_THREADS;
    //    return thread1 > thread2;
}
*/

EagerAction** CreateSingleLockingActionBatch(uint32_t numTxns, uint32_t txnSize, 
                                             uint64_t numRecords, 
                                             uint32_t experiment) { 
  std::cout << "Num records: " << numRecords << "\n";
  std::cout << "Txn size: " << txnSize << "\n";
  numLockingRecords = numRecords;
  EagerAction **ret = 
    (EagerAction**)alloc_mem(numTxns*sizeof(EagerAction*), 71);
  assert(ret != NULL);
  memset(ret, 0x0, numTxns*sizeof(EagerAction*));
  std::set<uint64_t> seenKeys; 

  EagerRecordInfo recordInfo;

  for (uint32_t i = 0; i < numTxns; ++i) {
    seenKeys.clear();
    EagerAction *action = new RMWEagerAction();    
    for (uint32_t j = 0; j < txnSize; ++j) {
      while (true) {
        uint64_t key = rand() % numRecords;
        if (seenKeys.find(key) == seenKeys.end()) {
          seenKeys.insert(key);
          recordInfo.is_write = true;
          recordInfo.record.key = key;

          if (experiment == 0) {
            action->writeset.push_back(recordInfo);
          }
          else if (experiment == 1) {
            if (j < 2) {
              action->writeset.push_back(recordInfo);
            }
            else {
              action->readset.push_back(recordInfo);
            }          
          }
          break;
        }
      }
    }
    //    std::sort(action->writeset.begin(), action->writeset.end());
    ret[i] = action;
  }
  return ret;
}

EagerActionBatch* SetupLockingInput(uint32_t txnSize, uint32_t numThreads, 
                                    uint32_t numTxns, uint32_t numRecords, 
                                    uint32_t experiment) {
  EagerActionBatch *ret = 
    (EagerActionBatch*)malloc(sizeof(EagerActionBatch)*numThreads);
  uint32_t txnsPerThread = numTxns/numThreads;
  for (uint32_t i = 0; i < numThreads; ++i) {
    EagerAction **actions = 
      CreateSingleLockingActionBatch(txnsPerThread, txnSize, numRecords, experiment);
    ret[i] = {
      txnsPerThread,
      actions,
    };
  }
  return ret;
}

EagerWorker** SetupLockThreads(SimpleQueue<EagerActionBatch> **inputQueue, 
                               SimpleQueue<EagerActionBatch> **outputQueue, 
                               uint32_t allocatorSize, 
                               LockManager *mgr, 
                               int numThreads,
                               Table **tables) {
  EagerWorker **ret = (EagerWorker**)malloc(sizeof(EagerWorker*)*numThreads);
  assert(ret != NULL);
  for (int i = 0; i < numThreads; ++i) {
    struct EagerWorkerConfig conf = {
      mgr, 
      inputQueue[i],
      outputQueue[i],
      i,
      100,
      tables,
    };
    ret[i] = new (i) EagerWorker(conf);
  }
  return ret;
}



void LockingExperiment(LockingConfig config) {
  
  // Setup input queues
  SimpleQueue<EagerActionBatch> **inputs = 
    (SimpleQueue<EagerActionBatch>**)malloc(sizeof(SimpleQueue<EagerActionBatch>*)
                                            *config.numThreads);
  for (uint32_t i = 0; i < config.numThreads; ++i) {
    char *data = (char*)alloc_mem(CACHE_LINE*1024, 71);
    inputs[i] = new SimpleQueue<EagerActionBatch>(data, 1024);
  }

  // Setup output queues
  SimpleQueue<EagerActionBatch> **outputs = 
    (SimpleQueue<EagerActionBatch>**)malloc(sizeof(SimpleQueue<EagerActionBatch>*)
                                           *config.numThreads);
  for (uint32_t i = 0; i < config.numThreads; ++i) {
    char *data = (char*)alloc_mem(CACHE_LINE*1024, 71);
    outputs[i] = new SimpleQueue<EagerActionBatch>(data, 1024);
  }

  // Setup the lock manager
  uint64_t *tableSizes = (uint64_t*)malloc(sizeof(uint64_t));
  *tableSizes = (uint64_t)config.numRecords;
  uint32_t numTables = 1;
  LockManagerConfig cfg = {
    1,
    tableSizes,
    0,
    (int)(config.numThreads-1),
    (1<<30)/sizeof(TxnQueue),
  };  
  unordered_map<uint32_t, uint64_t> tblInfo;
  tblInfo[0] = config.numRecords;
  LockManager *mgr = new LockManager(cfg);

  // Setup tables
  char bigval[1000];
  TableConfig tblConfig = {
    0,
    (uint64_t)config.numRecords,
    0,
    (int)(config.numThreads-1),
    2*(uint64_t)(config.numRecords),
    recordSize,
  };
  Table **tables = (Table**)malloc(sizeof(Table*));
  tables[0] = new (0) Table(tblConfig);
  for (uint64_t i = 0; i < config.numRecords; ++i) {
    if (recordSize == 1000) {
      uint64_t *bigInt = (uint64_t*)bigval;
      for (uint32_t j = 0; j < 125; ++j) {
        bigInt[j] = (uint64_t)rand();
      }
      tables[0]->Put(i, bigval);
    }
    else if (recordSize == 8) {
      tables[0]->Put(i, &i);
    }
  }
  tables[0]->SetInit();

  uint64_t counter = 0;
  if (recordSize == 8) {
    for (uint64_t i = 0; i < config.numRecords; ++i) {
      counter += *(uint64_t*)(tables[0]->Get(i));
    }
  }
  
  std::cout << "Finished table init. Counter: " << counter << "\n";

  // Setup worker threads
  EagerWorker **threads = SetupLockThreads(inputs, outputs, 256, mgr, 
                                           config.numThreads,
                                           tables);

  // Setup input
  EagerActionBatch *batches = SetupLockingInput(config.txnSize, 
                                                config.numThreads,
                                                config.numTxns,
                                                config.numRecords,
                                                config.experiment);
  int success = pin_thread(79);
  assert(success == 0);
  
  for (uint32_t i = 0; i < config.numThreads; ++i) {
      threads[i]->Run();
  }
  
  barrier();

  timespec start_time, end_time, elapsed_time;
  for (uint32_t i = 0; i < config.numThreads; ++i) {
    inputs[i]->EnqueueBlocking(batches[i]);

  }
  //  ProfilerStart("/home/jmf/multiversioning/locking.prof");
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);  
  for (uint32_t i = 0; i < config.numThreads; ++i) {
    outputs[i]->DequeueBlocking();
  }
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
  //    ProfilerStop();
  elapsed_time = diff_time(end_time, start_time);

  double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
  std::cout << elapsedMilli << '\n';
  std::ofstream resultFile;
  resultFile.open("locking.txt", std::ios::app | std::ios::out);
  resultFile << elapsedMilli << " " << config.numTxns << " ";
  resultFile << config.numThreads << "\n";
  //    std::cout << "Time elapsed: " << elapsedMilli << "\n";
  resultFile.close();  
}

// arg0: number of scheduler threads
// arg1: number of records in the database
// arg2: number of txns in an epoch
// arg3: number of epochs
int main(int argc, char **argv) {
  srand(time(NULL));
  /*
    assert(argc == 7);
    int numProcs = atoi(argv[1]);
    int numRecords = atoi(argv[2]);
    int epochSize = atoi(argv[3]);
    int numEpochs = atoi(argv[4]);
    int txnSize = atoi(argv[5]);
    int exptId = atoi(argv[6]);

    srand(time(NULL));

    if (exptId == 0) {
      DoExperiment(numProcs, numRecords, epochSize, numEpochs, txnSize);
    }
    else {
      DoHashes(numProcs, numRecords, epochSize, numEpochs, txnSize);
    }
    exit(0);
  */
  ExperimentConfig cfg(argc, argv);
  std::cout << cfg.ccType << "\n";
  if (cfg.ccType == MULTIVERSION) {
    MVScheduler::NUM_CC_THREADS = (uint32_t)cfg.mvConfig.numCCThreads;
    recordSize = cfg.mvConfig.recordSize;
    assert(recordSize == 8 || recordSize == 1000);
    DoExperiment(cfg.mvConfig.numCCThreads, cfg.mvConfig.numWorkerThreads, 
                 cfg.mvConfig.numRecords, 
                 cfg.mvConfig.epochSize, 
                 cfg.mvConfig.numTxns/cfg.mvConfig.epochSize, 
                 cfg.mvConfig.txnSize,
                 cfg.mvConfig.experiment);
    exit(0);
  }
  else {
    recordSize = cfg.lockConfig.recordSize;
    assert(recordSize == 8 || recordSize == 1000);
    LockingExperiment(cfg.lockConfig);
    exit(0);
  }
}
