#include <database.h>
#include <cpuinfo.h>
#include <config.h>
#include <eager_worker.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <gperftools/profiler.h>
#include <small_bank.h>
#include <setup_occ.h>
#include <setup_mv.h>
#include <setup_hek.h>
#include <algorithm>
#include <fstream>
#include <set>
#include <iostream>
#include <common.h>
#include <sys/mman.h>

#define RECYCLE_QUEUE_SIZE 64
#define INPUT_SIZE 1024
#define OFFSET 0
#define OFFSET_CORE(x) (x+OFFSET)

Database DB(2);

uint64_t dbSize = ((uint64_t)1<<36);

uint32_t NUM_CC_THREADS;

int NumProcs;
uint32_t numLockingRecords;
uint64_t recordSize;

EagerAction** CreateSingleLockingActionBatch(uint32_t numTxns, uint32_t txnSize,
                                             uint64_t numRecords, 
                                             uint32_t experiment,
                                             RecordGenerator *gen) { 
  std::cout << "Num records: " << numRecords << "\n";
  std::cout << "Txn size: " << txnSize << "\n";
  numLockingRecords = numRecords;
  EagerAction **ret = 
    (EagerAction**)alloc_mem(numTxns*sizeof(EagerAction*), 71);
  assert(ret != NULL);
  memset(ret, 0x0, numTxns*sizeof(EagerAction*));
  std::set<uint64_t> seenKeys; 

  EagerRecordInfo recordInfo;
  char temp_buf[248];

  for (uint32_t i = 0; i < numTxns; ++i) {
    seenKeys.clear();
    int flip = rand() % 100;
    if (experiment == 2) {
            GenRandomSmallBank(temp_buf, METADATA_SIZE);
        int txnType = rand() % 1;
        if (txnType == 0) {             // Balance
          uint64_t customer = (uint64_t)(rand() % numRecords);

          ret[i] = new LockingSmallBank::Balance(customer, numRecords, 
                                                 temp_buf);
        }
        else if (txnType == 1) {        // DepositChecking
          uint64_t customer = (uint64_t)(rand() % numRecords);
          long amount = (long)(rand() % 25);
          ret[i] = new LockingSmallBank::DepositChecking(customer, amount, 
                                                         numRecords,
                                                         temp_buf);

        }
        else if (txnType == 2) {        // TransactSaving
          uint64_t customer = (uint64_t)(rand() % numRecords);
          long amount = (long)(rand() % 25);
          ret[i] = new LockingSmallBank::TransactSaving(customer, amount, 
                                                        numRecords,
                                                        temp_buf);
        }
        else if (txnType == 3) {        // Amalgamate
          uint64_t fromCustomer = (uint64_t)(rand() % numRecords);
          uint64_t toCustomer;
          do {
            toCustomer = (uint64_t)(rand() % numRecords);
          } while (toCustomer == fromCustomer);
          ret[i] = new LockingSmallBank::Amalgamate(fromCustomer, toCustomer, 
                                                    numRecords,
                                                    temp_buf);
        }
        else if (txnType == 4) {        // WriteCheck
          uint64_t customer = (uint64_t)(rand() % numRecords);
          long amount = (long)(rand() % 25);
          if (rand() % 2 == 0) {
            amount *= -1;
          }
          ret[i] = new LockingSmallBank::WriteCheck(customer, amount, 
                                                    numRecords,
                                                    temp_buf);
        }      
    }
    else if (experiment < 2) {

      EagerAction *action = new RMWEagerAction();    
      for (uint32_t j = 0; j < txnSize; ++j) {
        while (true) {
          uint64_t key = gen->GenNext();
          if (seenKeys.find(key) == seenKeys.end()) {
            seenKeys.insert(key);
            recordInfo.is_write = true;
            recordInfo.record.key = key;
            recordInfo.record.hash = recordInfo.record.Hash() % numRecords;
            if (experiment == 0) {
//                    if (flip < 50) {
//                            action->readset.push_back(recordInfo);
//                            action->shadowReadset.push_back(recordInfo);
//                    } else if (j < 5) {

                    recordInfo.is_write = true;
                    action->shadowWriteset.push_back(recordInfo);
                    action->writeset.push_back(recordInfo);
                            //                    }
            }
            else if (experiment == 1) {
//                    if (flip < 0) {
//                            recordInfo.is_write = false;
//                            action->readset.push_back(recordInfo);
//                            action->shadowReadset.push_back(recordInfo);
                    if (j < 2) {
                            recordInfo.is_write = true;
                            action->writeset.push_back(recordInfo);
                            action->shadowWriteset.push_back(recordInfo);
                    } else {
                            recordInfo.is_write = false;
                            action->readset.push_back(recordInfo);
                            action->shadowReadset.push_back(recordInfo);
                    }
            }
            

          }
          break;
        }

      }
      //    std::sort(action->writeset.begin(), action->writeset.end(), LockManager::SortCmp);
      ret[i] = action;
    }
    
    for (uint32_t k = 0; k < ret[i]->writeset.size(); ++k) {
      uint32_t tableId = ret[i]->writeset[k].record.tableId;
      ret[i]->writeset[k].record.hash = 
        ret[i]->writeset[k].record.Hash() % LockManager::tableSizes[tableId];
        
    }
    for (uint32_t k = 0; k < ret[i]->readset.size(); ++k) {
      uint32_t tableId = ret[i]->readset[k].record.tableId;
      ret[i]->readset[k].record.hash = 
        ret[i]->readset[k].record.Hash() % LockManager::tableSizes[tableId];
    }
  }
  return ret;
}


EagerActionBatch* SetupLockingInput(uint32_t txnSize, uint32_t numThreads, 
                                    uint32_t numTxns, uint32_t numRecords, 
                                    uint32_t experiment,
                                    uint32_t distribution,
                                    double theta)
{
  RecordGenerator *gen = NULL;
  if (distribution == 0) {
    gen = new UniformGenerator(numRecords);
  }
  else if (distribution == 1) {
    gen = new ZipfGenerator(numRecords, theta);
  }
  EagerActionBatch *ret = 
    (EagerActionBatch*)malloc(sizeof(EagerActionBatch)*numThreads);
  uint32_t txnsPerThread = numTxns/numThreads;
  for (uint32_t i = 0; i < numThreads; ++i) {
    EagerAction **actions = 
      CreateSingleLockingActionBatch(txnsPerThread, txnSize, numRecords, 
                                     experiment,
                                     gen);
    ret[i] = {
      txnsPerThread,
      actions,
    };
  }
  return ret;
}

EagerWorker** SetupLockThreads(SimpleQueue<EagerActionBatch> **inputQueue, 
                               SimpleQueue<EagerActionBatch> **outputQueue, 
                               LockManager *mgr, 
                               int numThreads,
                               Table **tables) {
  assert(mgr != NULL);
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
  LockManager *mgr = NULL;
  Table **tables = NULL;
  if (config.experiment < 2) {
    uint64_t *tableSizes = (uint64_t*)malloc(sizeof(uint64_t));
    *tableSizes = (uint64_t)config.numRecords;
    uint32_t numTables = 1;
    LockManagerConfig cfg = {
            numTables,
      tableSizes,
      0,
      (int)(config.numThreads-1),
      (1<<30)/sizeof(TxnQueue),
    };  
    LockManager::tableSizes = tableSizes;
    mgr = new LockManager(cfg);

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

    tables = (Table**)malloc(sizeof(Table*));
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
  }
  else if (config.experiment == 2) {

    uint64_t *tableSizes = (uint64_t*)malloc(2*sizeof(uint64_t));
    tableSizes[0] = (uint64_t)config.numRecords;
    tableSizes[1] = (uint64_t)config.numRecords;

    uint32_t numTables = 2;
    LockManagerConfig cfg = {
      numTables,
      tableSizes,
      0,
      (int)(config.numThreads-1),
      (1<<30)/sizeof(TxnQueue),
    };  
    mgr = new LockManager(cfg);
    LockManager::tableSizes = tableSizes;

    // Savings table config
    TableConfig savingsCfg = {
      SAVINGS,
      (uint64_t)config.numRecords,
      0,
      (int)(config.numThreads-1),
      (uint64_t)(config.numRecords),
      sizeof(SmallBankRecord),
    };

    // Checking table config
    TableConfig checkingCfg = {
      CHECKING,
      (uint64_t)config.numRecords,
      0,
      (int)(config.numThreads-1),
      (uint64_t)(config.numRecords),
      sizeof(SmallBankRecord),
    };

    tables = (Table**)malloc(sizeof(Table*)*2);    
    tables[SAVINGS] = new(0) Table(savingsCfg);
    tables[CHECKING] = new(0) Table(checkingCfg);
    for (uint32_t i = 0; i < config.numRecords; ++i) {
      SmallBankRecord sbRecord;
      sbRecord.amount = (long)(rand() % 100);
      tables[SAVINGS]->Put((uint64_t)i, &sbRecord);

      sbRecord.amount = (long)(rand() % 100);
      tables[CHECKING]->Put((uint64_t)i, &sbRecord);
    }
    tables[SAVINGS]->SetInit();
    tables[CHECKING]->SetInit();
  }
  else {
    assert(false);
  }
  std::cout << "Finished table init. \n";

  // Setup worker threads
  EagerWorker **threads = SetupLockThreads(inputs, outputs, mgr, 
                                           config.numThreads,
                                           tables);

  // Setup input
  EagerActionBatch *batches = SetupLockingInput(config.txnSize, 
                                                config.numThreads,
                                                config.numTxns,
                                                config.numRecords,
                                                config.experiment,
                                                config.distribution,
                                                config.theta);
  int success = pin_thread(79);
  assert(success == 0);


  
  for (uint32_t i = 0; i < config.numThreads; ++i) {
      threads[i]->Run();
      threads[i]->WaitInit();
  }

  pin_memory();
  barrier();

  timespec start_time, end_time, elapsed_time;
  //  ProfilerStart("/home/jmf/multiversioning/locking.prof");
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);  
  for (uint32_t i = 0; i < config.numThreads; ++i) {
    inputs[i]->EnqueueBlocking(batches[i]);
  }

  for (uint32_t i = 0; i < config.numThreads; ++i) {
    outputs[i]->DequeueBlocking();
  }
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
  //  ProfilerStop();
  elapsed_time = diff_time(end_time, start_time);

  double elapsedMilli = 1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
  std::cout << elapsedMilli << '\n';
  std::ofstream resultFile;
  resultFile.open("locking.txt", std::ios::app | std::ios::out);

  resultFile << "time:" << elapsedMilli << " txns:" << config.numTxns << " ";
  resultFile << "threads:" << config.numThreads << " locking ";
  resultFile << "records:" << config.numRecords << " ";
  if (config.experiment == 0) {
    resultFile << "10rmw" << " ";
  }
  else if (config.experiment == 1) {
    resultFile << "8r2rmw" << " ";
  }
  else if (config.experiment == 2) {
    resultFile << "small_bank" << " ";
  }
 
  if (config.distribution == 0) {
    resultFile << "uniform" << "\n";
  }
  else if (config.distribution == 1) {
    resultFile << "zipf theta:" << config.theta << "\n";
  }  

  //    std::cout << "Time elapsed: " << elapsedMilli << "\n";
  resultFile.close();  
}

// arg0: number of scheduler threads
// arg1: number of records in the database
// arg2: number of txns in an epoch
// arg3: number of epochs
int main(int argc, char **argv) {
        //        mlockall(MCL_FUTURE);
  srand(time(NULL));
  ExperimentConfig cfg(argc, argv);
  std::cout << cfg.ccType << "\n";
  if (cfg.ccType == MULTIVERSION) {
          if (cfg.mvConfig.experiment < 3) 
                  recordSize = cfg.mvConfig.recordSize;
          else if (cfg.mvConfig.experiment < 5)
                  recordSize = sizeof(SmallBankRecord);
          else
                  assert(false);
          do_mv_experiment(cfg.mvConfig);
          exit(0);
  } else if (cfg.ccType == LOCKING) {
          recordSize = cfg.lockConfig.recordSize;
          assert(cfg.lockConfig.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          LockingExperiment(cfg.lockConfig);
          exit(0);
  } else if (cfg.ccType == OCC) {
          recordSize = cfg.occConfig.recordSize;
          assert(cfg.occConfig.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          occ_experiment(cfg.occConfig);
          exit(0);
  } else if (cfg.ccType == HEK) {
          recordSize = cfg.hek_conf.record_size;
          assert(cfg.hek_conf.distribution < 2);
          assert(recordSize == 8 || recordSize == 1000);
          do_hekaton_experiment(cfg.hek_conf);
          exit(0);
  }
}
