#ifndef         CONFIG_H_
#define         CONFIG_H_

#include <getopt.h>
#include <stdlib.h>
#include <unordered_map>

static struct option long_options[] = {
  {"epoch_size", required_argument, NULL, 0},
  {"num_txns", required_argument, NULL, 1},
  {"num_lock_threads", required_argument, NULL, 2},
  {"num_worker_threads", required_argument, NULL, 3},
  {"num_cc_threads", required_argument, NULL, 4},
  {"num_records", required_argument, NULL, 5},
  {"num_contended", required_argument, NULL, 6},
  {"txn_size", required_argument, NULL, 7},
  {"cc_type", required_argument, NULL, 8},
  {NULL, no_argument, NULL, 9},
};


enum ConcurrencyControl {
  MULTIVERSION = 0,
  LOCKING,
};

struct LockingConfig {
  uint32_t numThreads;
  uint32_t numTxns;
  uint32_t numRecords;
  uint32_t numContendedRecords;
  uint32_t txnSize;
};

struct MVConfig {
  uint32_t numCCThreads;
  uint32_t numTxns;
  uint32_t epochSize;
  uint32_t numRecords;
  uint32_t numWorkerThreads;
  uint32_t txnSize;
};

class ExperimentConfig {
 private:
  enum OptionCode {
    EPOCH_SIZE = 0,
    NUM_TXNS,
    NUM_LOCK_THREADS,
    NUM_WORKER_THREADS,
    NUM_CC_THREADS,
    NUM_RECORDS,
    NUM_CONTENDED,
    TXN_SIZE,
    CC_TYPE,
  };
  unordered_map<int, char*> argMap;

 public:

  ConcurrencyControl ccType;
  LockingConfig lockConfig;
  MVConfig mvConfig;    

  ExperimentConfig(int argc, char **argv) {
    ReadArgs(argc, argv);
    InitConfig();
  }

  void InitConfig() {
    int ccType = -1;
    if ((argMap.count(CC_TYPE) == 0) || 
        ((ccType = atoi(argMap[CC_TYPE])) != MULTIVERSION && 
         ccType != LOCKING)) {
      std::cerr << "Undefined concurrency control type\n";
      exit(-1);
    }

    if (ccType == MULTIVERSION) {
      if (argMap.count(NUM_CC_THREADS) == 0 ||
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(EPOCH_SIZE) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_WORKER_THREADS) == 0 ||
          argMap.count(TXN_SIZE) == 0) {
        std::cerr << "Missing one or more multiversion concurrency control params\n";
        std::cerr << "--" << long_options[NUM_CC_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[EPOCH_SIZE].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_WORKER_THREADS].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        exit(-1);        
      }

      mvConfig.numCCThreads = (uint32_t)atoi(argMap[NUM_CC_THREADS]);
      mvConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      mvConfig.epochSize = (uint32_t)atoi(argMap[EPOCH_SIZE]);
      mvConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      mvConfig.numWorkerThreads = (uint32_t)atoi(argMap[NUM_WORKER_THREADS]);
      mvConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      this->ccType = MULTIVERSION;
    }
    else {  // ccType == LOCKING
      
      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_CONTENDED) == 0 ||
          argMap.count(TXN_SIZE) == 0) {
        
        std::cerr << "Missing one or more locking concurrency control params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_CONTENDED].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        exit(-1);
      }

      
      lockConfig.numThreads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      lockConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      lockConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      lockConfig.numContendedRecords = 
        (uint32_t)atoi(argMap[NUM_CONTENDED]);
      this->ccType = LOCKING;
    }
  }

  void ReadArgs(int argc, char **argv) {

    int index = -1;
    while (getopt_long_only(argc, argv, "", long_options, &index) != -1) {
      if (index != -1 && argMap.count(index) == 0) {  // Correct argument
        argMap[index] = optarg;
      }
      
      else if (index == -1) { // Unknown argument
        std::cerr << "Error. Unknown argument\n";
        exit(-1);
      }
      else {  // Duplicate argument
        std::cerr << "Error duplicate argument\n";// <<  << "\n";
        exit(-1);
      }
      index = -1;
    }
  }
};

#endif          // CONFIG_H_