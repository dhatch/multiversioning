#include <config.h>
#include <common.h>
#include <mv_action.h>
#include <small_bank.h>
#include <concurrent_queue.h>
#include <preprocessor.h>
#include <executor.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <iostream>
#include <fstream>
#include <gperftools/profiler.h>

#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

#define MV_DRY_RUNS 5

static uint64_t dbSize = ((uint64_t)1<<36);

static void CreateQueues(int cpuNumber, uint32_t subCount, 
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


static MVSchedulerConfig SetupSched(int cpuNumber, 
                                    uint32_t threadId, 
                                    int numThreads, 
                                    size_t alloc, 
                                    uint32_t numTables,
                                    size_t *partSizes, 
                                    uint32_t numRecycles,
                                    SimpleQueue<ActionBatch> *inputQueue,
                                    uint32_t numOutputs,
                                    SimpleQueue<ActionBatch> *outputQueues,
                                    int worker_start,
                                    int worker_end) {
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
                numTables,
                partSizes,
                numOutputs,
                subCount,
                numRecycles,
                inputQueue,
                outputQueues,
                pubQueues,
                subQueues,
                queueArray,
                worker_start,
                worker_end,
        };
        return cfg;
}

static GarbageBinConfig SetupGCConfig(uint32_t numCCThreads,
                                      uint32_t numWorkerThreads,
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
static SimpleQueue<RecordList>* SetupGCQueues(uint32_t cpuNumber, 
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
        //        uint32_t qSz = sizeof(SimpleQueue<RecordList>);
        uint32_t singleDataSz = CACHE_LINE*RECYCLE_QUEUE_SIZE;
  
        // Initialize queues
        for (uint32_t i = 0; i < numTables; ++i) {
                for (uint32_t j = 0; j < queuesPerTable; ++j) {
      
                        uint32_t queueOffset = (i*queuesPerTable + j);
                        uint32_t dataOffset = queueOffset*singleDataSz;
                        new (&queueData[queueOffset]) 
                                SimpleQueue<RecordList>(data + dataOffset, RECYCLE_QUEUE_SIZE);        
                }
        }
        return queueData;
}

static ExecutorConfig SetupExec(uint32_t cpuNumber, uint32_t threadId, 
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
    (int)cpuNumber,
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

static Executor** SetupExecutors(uint32_t cpuStart,
                                 uint32_t numWorkers, 
                                 uint32_t numCCThreads,
                                 uint32_t queuesPerTable,
                                 SimpleQueue<ActionBatch> *inputQueue,
                                 SimpleQueue<ActionBatch> *outputQueue,
                                 uint32_t queuesPerCCThread,
                                 SimpleQueue<MVRecordList> ***ccQueues) {  
  assert(queuesPerCCThread == numWorkers);
  assert(queuesPerTable == numWorkers);

  uint64_t threadDbSz = dbSize / numWorkers;
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
static SimpleQueue<T>* SetupQueuesMany(uint32_t numEntries, uint32_t numQueues, int cpu) {
  size_t metaDataSz = sizeof(SimpleQueue<T>)*numQueues; // SimpleQueue structs
  size_t queueDataSz = CACHE_LINE*numEntries*numQueues; // queue data
  size_t totalSz = metaDataSz+queueDataSz;
  //SimpleQueue<T> *queue_array = alloc_mem(sizeof(SimpleQueue<T>)*numQueues);
  void *data = alloc_mem(totalSz, cpu);
  memset(data, 0x00, totalSz);

  char *queueData = (char*)data + metaDataSz;
  size_t dataDelta = CACHE_LINE*numEntries;
  SimpleQueue<T> *queues = (SimpleQueue<T>*)data;

  // Initialize queue structs
  for (uint32_t i = 0; i < numQueues; ++i) {
      new (&queues[i]) SimpleQueue<T>(&queueData[dataDelta*i], numEntries);
  }
  return queues;
}

static MVScheduler** SetupSchedulers(int numProcs, 
                                     SimpleQueue<ActionBatch> **inputQueueRef_OUT, 
                                     SimpleQueue<ActionBatch> **outputQueueRefs_OUT, 
                                     uint32_t numOutputs,
                                     size_t allocatorSize, 
                                     uint32_t numTables,
                                     size_t tableSize, 
                                     SimpleQueue<MVRecordList> ***gcRefs_OUT,
                                     int worker_start, int worker_end) {  
        
  size_t partitionChunk = tableSize/numProcs;
  size_t *tblPartitionSizes = (size_t*)malloc(numTables*sizeof(size_t));
  for (uint32_t i = 0; i < numTables; ++i) {
    tblPartitionSizes[i] = partitionChunk;
  }

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
                                                    numTables,
                                                    tblPartitionSizes, 
                                                    numOutputs,
                                                    leaderInputQueue,
                                                    numOutputs,
                                                    leaderOutputQueues,
                                                    worker_start, worker_end);

  schedArray[0] = 
    new (globalLeaderConfig.cpuNumber) MVScheduler(globalLeaderConfig);
  gcRefs_OUT[0] = globalLeaderConfig.recycleQueues;

  MVSchedulerConfig localLeaderConfig = globalLeaderConfig;
  
  for (uint32_t i = 1; i < numProcs; ++i) {
    if (i % 10 == 0) {
      int leaderNum = i/10;
      auto inputQueue = globalLeaderConfig.pubQueues[9+leaderNum-1];
      auto outputQueue = globalLeaderConfig.subQueues[9+leaderNum-1];
      MVSchedulerConfig config = SetupSched(i, i, numProcs, allocatorSize, 
                                            numTables,
                                            tblPartitionSizes, 
                                            numOutputs,
                                            inputQueue, 
                                            1,
                                            outputQueue, worker_start,
                                            worker_end);
      schedArray[i] = new (config.cpuNumber) MVScheduler(config);
      gcRefs_OUT[i] = config.recycleQueues;
      localLeaderConfig = config;
    }
    else {
      int index = i%10;      
      auto inputQueue = localLeaderConfig.pubQueues[index-1];
      auto outputQueue = localLeaderConfig.subQueues[index-1];
      MVSchedulerConfig subConfig = SetupSched(i, i, numProcs, allocatorSize, 
                                               numTables,
                                               tblPartitionSizes, 
                                               numOutputs,
                                               inputQueue, 
                                               1,
                                               outputQueue, worker_start,
                                               worker_end);
      schedArray[i] = new (subConfig.cpuNumber) MVScheduler(subConfig);
      gcRefs_OUT[i] = subConfig.recycleQueues;
    }
  }
  
  *inputQueueRef_OUT = leaderInputQueue;
  *outputQueueRefs_OUT = leaderOutputQueues;
  return schedArray;
}

static CompositeKey create_mv_key(uint32_t table_id, uint64_t key, bool is_rmw)
{
        CompositeKey mv_key(is_rmw, table_id, key);
        mv_key.threadId =
                CompositeKey::HashKey(&mv_key) % MVScheduler::NUM_CC_THREADS;
        mv_key.value = NULL;
        mv_key.next = -1;
        return mv_key;
}

static Action* generate_small_bank_action(uint32_t num_records, bool read_only)
{
        Action *action;
        char *temp_buf;
        int mod, txn_type;
        long amount;
        uint64_t customer, from_customer, to_customer;
        if (read_only == true) {
                mod = 1;
        } else {
                mod = 5;
        }
        temp_buf = (char*)malloc(METADATA_SIZE);
        GenRandomSmallBank(temp_buf, METADATA_SIZE);
        txn_type = rand() % mod;
        if (txn_type == 0) {
                customer = (uint64_t)(rand() % num_records);
                action = new MVSmallBank::Balance(customer, temp_buf);
        } else if (txn_type == 1) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                action = new MVSmallBank::DepositChecking(customer, amount,
                                                       temp_buf);
        } else if (txn_type == 2) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                action = new MVSmallBank::TransactSaving(customer, amount,
                                                      temp_buf);
        } else if (txn_type == 3) {
                from_customer = (uint64_t)(rand() % num_records);
                do {
                        to_customer = (uint64_t)(rand() % num_records);
                } while (to_customer == from_customer);
                action = new MVSmallBank::Amalgamate(from_customer, to_customer,
                                                   temp_buf);
        } else if (txn_type == 4) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                if (rand() % 2 == 0) {
                        amount *= -1;
                }
                action = new MVSmallBank::WriteCheck(customer, amount, temp_buf);
        } else {
                assert(false);
        }
        return action;
}


static Action* generate_single_rmw_action(RecordGenerator *generator,
                                          uint32_t num_writes,
                                          uint32_t num_rmws,
                                          uint32_t num_reads)
{
        Action *action;
        uint32_t i;
        uint64_t key;
        CompositeKey mv_key(true);
        std::set<uint64_t> seen_keys;
        int indices[NUM_CC_THREADS];
        int index;
        int *ptr;
                
        action = new RMWAction((uint64_t)rand());
        for(i = 0; i < NUM_CC_THREADS; ++i)
                indices[i] = -1;
        assert(action->__combinedHash == 0);
        for (i = 0; i < num_writes; ++i) {
                key = GenUniqueKey(generator, &seen_keys);
                mv_key = create_mv_key(0, key, false);
                action->__writeset.push_back(mv_key);
                action->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
                if (indices[mv_key.threadId] != -1) {
                        index = indices[mv_key.threadId];
                        ptr = &action->__writeset[index].next;
                } else {
                        ptr = &action->__write_starts[mv_key.threadId];
                }
                indices[mv_key.threadId] = i;
                *ptr = i;
        }
        for (i = 0; i < num_rmws; ++i) {
                key = GenUniqueKey(generator, &seen_keys);
                mv_key = create_mv_key(0, key, true);
                action->__writeset.push_back(mv_key);
                action->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
                if (indices[mv_key.threadId] != -1) {
                        index = indices[mv_key.threadId];
                        ptr = &action->__writeset[index].next;
                } else {
                        ptr = &action->__write_starts[mv_key.threadId];
                }
                indices[mv_key.threadId] = i;
                *ptr = i;
        }
        for (i = 0; i < NUM_CC_THREADS; ++i)
                indices[i] = -1;
        
        for (i = 0; i < num_reads; ++i) {
                key = GenUniqueKey(generator, &seen_keys);
                mv_key = create_mv_key(0, key, false);
                action->__readset.push_back(mv_key);
                action->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
                if (indices[mv_key.threadId] != -1) {
                        index = indices[mv_key.threadId];
                        ptr = &action->__readset[index].next;
                } else {
                        ptr = &action->__read_starts[mv_key.threadId];
                }
                indices[mv_key.threadId] = i;
                *ptr = i;
        }
        return action;
}

static Action* generate_readonly(RecordGenerator *gen, MVConfig config)
{
        mv_readonly *act;
        int i;
        uint64_t key;
        CompositeKey mv_key;
        std::set<uint64_t> seen_keys;
        act = new mv_readonly();
        for (i = 0; i < config.read_txn_size; ++i) {
                key = GenUniqueKey(gen, &seen_keys);
                mv_key = create_mv_key(0, key, false);
                act->__readset.push_back(mv_key);
                act->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
        }
        return act;
}

static Action* generate_mix(RecordGenerator *gen, MVConfig config, bool is_rmw)
{
        mv_mix_action *act;
        uint32_t i;
        uint64_t key;
        CompositeKey mv_key;
        std::set<uint64_t> seen_keys;
        act = new mv_mix_action();
        act->__readonly = false;
        for (i = 0; i < RMW_COUNT; ++i) {
                key = GenUniqueKey(gen, &seen_keys);
                mv_key = create_mv_key(0, key, is_rmw);
                act->__writeset.push_back(mv_key);
                act->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
        }
        for (i = 0; i < config.txnSize - RMW_COUNT; ++i) {
                key = GenUniqueKey(gen, &seen_keys);
                mv_key = create_mv_key(0, key, false);
                act->__readset.push_back(mv_key);
                act->__combinedHash |= (((uint64_t)1)<<mv_key.threadId);
        }
        return act;
        
}

static Action* generate_rmw_action(RecordGenerator *gen, MVConfig config)
{
        uint32_t num_reads, num_rmws, num_writes;
        int flip;
        assert(RMW_COUNT <= config.txnSize);
        flip = (uint32_t)rand() % 100;
        assert(flip >= 0 && flip < 100);
        if (flip < config.read_pct) {
                return generate_readonly(gen, config);
        } else if (config.experiment == 0) {
                num_writes = 0;
                num_rmws = config.txnSize;
                num_reads = 0;
        } else if (config.experiment == 1) {
                num_writes = 0;
                num_rmws = RMW_COUNT;
                num_reads = config.txnSize - RMW_COUNT;
        } else if (config.experiment == 2) {
                return generate_mix(gen, config, false);
        }
        return generate_single_rmw_action(gen, num_writes, num_rmws, num_reads);
}

static uint32_t get_num_epochs(MVConfig config)
{
        uint32_t num_epochs;
        num_epochs = config.numTxns/config.epochSize;
        if (config.numTxns % config.epochSize) {
                num_epochs += 1;
        }
        return num_epochs;
}

static ActionBatch mv_create_action_batch(RecordGenerator *gen, MVConfig config,
                                          uint32_t epoch)
{
        ActionBatch batch;
        Action *action;
        uint32_t i;
        uint64_t timestamp;
        batch.numActions = config.epochSize;
        batch.actionBuf = (Action**)malloc(sizeof(Action*)*config.epochSize);
        assert(batch.actionBuf != NULL);
        for (i = 0; i < config.epochSize; ++i) {
                timestamp = CREATE_MV_TIMESTAMP(epoch, i);
                if (config.experiment == 3) {
                        action = generate_small_bank_action(config.numRecords,
                                                            false);
                } else if (config.experiment == 4) {
                        action = generate_small_bank_action(config.numRecords,
                                                            true);
                } else if (config.experiment < 3) {
                        action = generate_rmw_action(gen, config);
                } else {
                        assert(false);
                }
                action->__version = timestamp;
                batch.actionBuf[i] = action;
        }
        return batch;
}

static void mv_setup_input_array(std::vector<ActionBatch> *input,
                                 MVConfig config)
{
        uint32_t num_epochs;
        RecordGenerator *gen;
        ActionBatch batch;
        uint32_t i;
        if (config.distribution == 0) {
                gen = new UniformGenerator((uint32_t)config.numRecords);
        } else if (config.distribution == 1) {
                gen = new ZipfGenerator((uint64_t)config.numRecords,
                                        config.theta);
        }
        num_epochs = get_num_epochs(config);

        for (i = 0; i < num_epochs + MV_DRY_RUNS; ++i) {
                batch = mv_create_action_batch(gen, config, i+2);
                input->push_back(batch);
        }
        std::cerr << "Done setting up mv input!\n";
}

static ActionBatch generate_small_bank_db(uint64_t num_customers,
                                          uint32_t num_threads)
{
        ActionBatch batch;
        uint32_t i;
        uint64_t records_per_thread, remainder, start;
        Action *cur_action, **txns;
        records_per_thread = num_customers / num_threads;
        remainder = num_customers % num_threads;
        txns = (Action**)malloc(sizeof(Action*)*num_threads);
        start = 0;
        for (i = 0; i < num_threads; ++i) {
                if (i == num_threads - 1) {
                        records_per_thread += remainder;
                }
                cur_action = new MVSmallBank::LoadCustomerRange(start,
                                                                start+records_per_thread);
                cur_action->__version = CREATE_MV_TIMESTAMP(1, i);
                start += records_per_thread;
        }
        batch = {
                txns,
                num_threads,
        };
        return batch;        
}

static InsertAction* generate_single_insert_txn(uint64_t start_record,
                                                uint64_t end_record)
{
        uint64_t i;
        uint32_t threadId;
        InsertAction *action;
        CompositeKey composite_key(false, 0, 0);
        int indices[NUM_CC_THREADS];
        int index;
        int *ptr;
        for(i = 0; i < NUM_CC_THREADS; ++i)
                indices[i] = -1;

        action = new InsertAction();
        action->__state = STICKY;
        action->__combinedHash = 0;
        action->__readonly = false;
        for (i = start_record; i < end_record; ++i) {
                composite_key.key = i;
                composite_key.tableId = 0;
                composite_key.threadId =
                        CompositeKey::HashKey(&composite_key) % MVScheduler::NUM_CC_THREADS;
                action->__writeset.push_back(composite_key);
                threadId = composite_key.threadId;
                action->__combinedHash |= (((uint64_t)1)<<threadId);
                if (indices[threadId] != -1) {
                        index = indices[threadId];
                        ptr = &action->__writeset[index].next;
                } else {
                        ptr = &action->__write_starts[threadId];
                }
                indices[threadId] = i-start_record;
                *ptr = i-start_record;
        }
        return action;
}

static ActionBatch generate_ycsb_db(uint64_t numRecords, uint32_t numThreads)
{
        uint64_t records_per_thread, remainder, cur;
        uint32_t i;
        ActionBatch batch;
        Action *action;
        batch.actionBuf = (Action**)malloc(sizeof(Action*)*numRecords);
        records_per_thread = numRecords / numThreads;
        remainder = numRecords % numThreads;
        cur = 0;
        for (i = 0; i < numThreads; ++i) {
                if (i == (numThreads-1))
                        records_per_thread += remainder;
                action = generate_single_insert_txn(cur,
                                                    cur+records_per_thread);
                action->__version = CREATE_MV_TIMESTAMP(1, i);
                batch.actionBuf[i] = action;
                cur += records_per_thread;
        }
        batch.numActions = numThreads;
        return batch;
}

static void write_results(MVConfig config, timespec elapsed_time)
{
        uint32_t num_epochs;
        double elapsed_milli;
        std::ofstream result_file;
        num_epochs = get_num_epochs(config);
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        std::cerr << "Number of txns: " << config.numTxns << "\n";
        std::cerr << "Time: " << elapsed_milli << "\n";
        result_file.open("results.txt", std::ios::app | std::ios::out);
        result_file << "mv ";
        result_file << "time:" << elapsed_milli << " ";
        result_file << "txns:" << (num_epochs-5)*config.epochSize << " ";
        result_file << "ccthreads:" << config.numCCThreads << " ";
        result_file << "workerthreads:" << config.numWorkerThreads << " ";
        result_file << "records:" << config.numRecords << " ";
        if (config.experiment == 0) {
                result_file << "10rmw ";
        } else if (config.experiment == 1) {
                result_file << "8r2rmw ";
        } else if (config.experiment == 2) {
                result_file << "5w ";
        } else if (config.experiment < 5) {
                result_file << "small_bank ";
        }        
        if (config.distribution == 0) {
                result_file << "uniform";
        } else if (config.distribution == 1) {
                result_file << "zipf theta:" << config.theta;
        }
        result_file << "\n";
        result_file.close();
        
}

static timespec run_experiment(SimpleQueue<ActionBatch> *input_queue,
                               SimpleQueue<ActionBatch> *output_queue,
                               std::vector<ActionBatch> inputs)
{
        uint32_t num_batches, i;
        struct timespec elapsed_time, end_time, start_time;
        num_batches = inputs.size();

        barrier();
        for (i = 0; i < MV_DRY_RUNS; ++i)
                input_queue->EnqueueBlocking(inputs[i]);
        for (i = 0; i < MV_DRY_RUNS; ++i)
                output_queue->DequeueBlocking();
        barrier();

        if (PROFILE)
                ProfilerStart("bohm.prof");
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        barrier();                
        for (i = MV_DRY_RUNS; i < num_batches; ++i) {
                input_queue->EnqueueBlocking(inputs[i]);
        }
        barrier();
        for (i = MV_DRY_RUNS; i < num_batches; ++i) {
                output_queue->DequeueBlocking();
        }
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        barrier();
        if (PROFILE)
                ProfilerStop();                
        elapsed_time = diff_time(end_time, start_time);
        std::cerr << "Done running Bohm experiment!\n";
        return elapsed_time;
}

static ActionBatch setup_loader_txns(MVConfig config)
{
        ActionBatch batch;
        if (config.experiment < 3) {
                batch = generate_ycsb_db(config.numRecords,
                                         config.numWorkerThreads);
        } else if (config.experiment < 5) {
                batch = generate_small_bank_db(config.numRecords,
                                               config.numWorkerThreads);
        } else {
                assert(false);
        }
        return batch;
}

static void init_database(MVConfig config,
                          SimpleQueue<ActionBatch> *input_queue,
                          SimpleQueue<ActionBatch> *output_queue,
                          MVScheduler **sched_threads,
                          Executor **exec_threads)
                          
{
        uint32_t i;
        ActionBatch init_batch;
        int pin_success;
        pin_success = pin_thread(79);
        assert(pin_success == 0);
        init_batch = setup_loader_txns(config);
        for (i = 0; i < config.numCCThreads; ++i) {
                sched_threads[i]->Run();        
                sched_threads[i]->WaitInit();
        }
        for (i = 0; i < config.numWorkerThreads; ++i) {
                exec_threads[i]->Run();
                exec_threads[i]->WaitInit();                
        }
        input_queue->EnqueueBlocking(init_batch);
        output_queue->DequeueBlocking();
        barrier();
        std::cerr << "Done loading the database!\n";
        return;
}

static MVScheduler** setup_scheduler_threads(MVConfig config,
                                             SimpleQueue<ActionBatch> **sched_input,
                                             SimpleQueue<ActionBatch> **sched_output,
                                             SimpleQueue<MVRecordList> ***gc_queues)
{
        uint64_t stickies_per_thread;
        uint32_t num_tables;
        MVScheduler **schedulers;
        int worker_start, worker_end;

        worker_start = (int)config.numCCThreads;
        worker_end = worker_start + config.numWorkerThreads - 1;
        if (config.experiment < 3) {
                stickies_per_thread = (((uint64_t)1)<<28);
                num_tables = 1;
        } else if (config.experiment < 5) {
                stickies_per_thread = (((uint64_t)1)<<24);
                num_tables = 2;
        } else {
                assert(false);
        }
        schedulers = SetupSchedulers(config.numCCThreads, sched_input,
                                     sched_output, config.numWorkerThreads+1,
                                     stickies_per_thread, num_tables,
                                     config.numRecords, gc_queues,
                                     worker_start,
                                     worker_end);
        assert(schedulers != NULL);
        assert(*sched_input != NULL);
        assert(*sched_output != NULL);
        std::cerr << "Done setting up scheduler threads!\n";
        std::cerr << "Num scheduler threads:";
        std::cerr << MVScheduler::NUM_CC_THREADS << "\n";
        return schedulers;
}

static Executor** setup_executors(MVConfig config,
                                  SimpleQueue<ActionBatch> *sched_outputs,
                                  SimpleQueue<ActionBatch> *output_queue,
                                  SimpleQueue<MVRecordList> ***gc_queues)
{
        uint32_t start_cpu, queues_per_table, queues_per_cc_thread;
        Executor **execs;
        start_cpu = config.numCCThreads;
        queues_per_table = config.numWorkerThreads;
        queues_per_cc_thread = config.numWorkerThreads;
        execs = SetupExecutors(start_cpu, config.numWorkerThreads,
                               config.numCCThreads, queues_per_table,
                               sched_outputs, output_queue,
                               queues_per_cc_thread, gc_queues);
        std::cerr << "Done setting up executors!\n";
        return execs;
}

void do_mv_experiment(MVConfig config)
{
        MVScheduler **schedThreads;
        Executor **execThreads;
        SimpleQueue<ActionBatch> *schedInputQueue;
        SimpleQueue<ActionBatch> *schedOutputQueues;
        SimpleQueue<MVRecordList> **schedGCQueues[config.numCCThreads];
        SimpleQueue<ActionBatch> *outputQueue;
        std::vector<ActionBatch> input_placeholder;
        timespec elapsed_time;
        
        MVScheduler::NUM_CC_THREADS = (uint32_t)config.numCCThreads;
        NUM_CC_THREADS = (uint32_t)config.numCCThreads;
        assert(config.distribution < 2);
        outputQueue = SetupQueuesMany<ActionBatch>(INPUT_SIZE, 1, 71);
        schedThreads = setup_scheduler_threads(config, &schedInputQueue,
                                               &schedOutputQueues,
                                               schedGCQueues);
        mv_setup_input_array(&input_placeholder, config);
        execThreads = setup_executors(config, schedOutputQueues, outputQueue,
                                      schedGCQueues);
        init_database(config, schedInputQueue, outputQueue, schedThreads,
                      execThreads);
        pin_memory();
        elapsed_time = run_experiment(schedInputQueue,
                                      outputQueue,
                                      input_placeholder);
        write_results(config, elapsed_time);
}

/*
void DoHashes(int numProcs, int numRecords, int epochSize, int numEpochs, 
              int txnSize) {
  char *inputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);            
  SimpleQueue<ActionBatch> *inputQueue = 
    new SimpleQueue<ActionBatch>(inputArray, INPUT_SIZE);
  char *outputArray = (char*)alloc_mem(CACHE_LINE*INPUT_SIZE, 71);
  SimpleQueue<ActionBatch> *outputQueue = esswriteset
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
  double elapsed_milli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
  std::cout << elapsed_milli << '\n';
  std::ofstream result_file;
  result_file.open("hashing.txt", std::ios::app | std::ios::out);
  result_file << elapsed_milli << " " << numEpochs*epochSize << " " << numProcs << "\n";
  //    std::cout << "Time elapsed: " << elapsed_milli << "\n";
  result_file.close();
}
*/

