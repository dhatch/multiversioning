#ifndef         CONFIG_H_
#define         CONFIG_H_

#include <getopt.h>
#include <stdlib.h>
#include <unordered_map>

using namespace std;

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
  {"experiment", required_argument, NULL, 9},
  {"record_size", required_argument, NULL, 10},
  {"distribution", required_argument, NULL, 11},
  {"theta", required_argument, NULL, 12},
  {"occ_epoch", required_argument, NULL, 13},
  {NULL, no_argument, NULL, 14},
};


enum ConcurrencyControl {
  MULTIVERSION = 0,
  LOCKING = 1,
  OCC,
};

struct OCCConfig {
        uint32_t numThreads;
        uint32_t numTxns;
        uint32_t numRecords;
        uint32_t numContendedRecords;
        uint32_t txnSize;
        uint32_t experiment;
        uint64_t recordSize;
        uint32_t distribution;
        double theta;
        bool globalTime;
        uint64_t occ_epoch;
};

struct LockingConfig {
  uint32_t numThreads;
  uint32_t numTxns;
  uint32_t numRecords;
  uint32_t numContendedRecords;
  uint32_t txnSize;
  uint32_t experiment;
  uint64_t recordSize;
  uint32_t distribution;
  double theta;
};

struct MVConfig {
  uint32_t numCCThreads;
  uint32_t numTxns;
  uint32_t epochSize;
  uint32_t numRecords;
  uint32_t numWorkerThreads;
  uint32_t txnSize;
  uint32_t experiment;
  uint64_t recordSize;
  uint32_t distribution;
  double theta;
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
    EXPERIMENT,
    RECORD_SIZE,
    DISTRIBUTION,
    THETA,
    OCC_EPOCH,
  };
  unordered_map<int, char*> argMap;

 public:
  ConcurrencyControl ccType;
  LockingConfig lockConfig;
  OCCConfig occConfig;
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
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0) {
        std::cerr << "Missing one or more multiversion concurrency control params\n";
        std::cerr << "--" << long_options[NUM_CC_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[EPOCH_SIZE].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_WORKER_THREADS].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        exit(-1);        
      }
      
      if (atoi(argMap[DISTRIBUTION]) == 1 && argMap.count(THETA) == 0) {
        std::cerr << "Zipfian config parameter, theta, missing!\n";
        exit(-1);
      }
      
      mvConfig.numCCThreads = (uint32_t)atoi(argMap[NUM_CC_THREADS]);
      mvConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      mvConfig.epochSize = (uint32_t)atoi(argMap[EPOCH_SIZE]);
      mvConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      mvConfig.numWorkerThreads = (uint32_t)atoi(argMap[NUM_WORKER_THREADS]);
      mvConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      mvConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      mvConfig.recordSize = (uint64_t)atoi(argMap[RECORD_SIZE]);
      mvConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      if (argMap.count(THETA) > 0) {
        mvConfig.theta = (double)atof(argMap[THETA]);
      }
      this->ccType = MULTIVERSION;
    } else if (ccType == LOCKING) {  // ccType == LOCKING
      
      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_CONTENDED) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0) {
        
        std::cerr << "Missing one or more locking concurrency control params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_CONTENDED].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        exit(-1);
      }

      if (atoi(argMap[DISTRIBUTION]) == 1 && argMap.count(THETA) == 0) {
        std::cerr << "Zipfian config parameter, theta, missing!\n";
        exit(-1);
      }
      
      lockConfig.numThreads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      lockConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      lockConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      lockConfig.numContendedRecords = 
        (uint32_t)atoi(argMap[NUM_CONTENDED]);
      lockConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      lockConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      lockConfig.recordSize = (uint64_t)atoi(argMap[RECORD_SIZE]);
      lockConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      if (argMap.count(THETA) > 0) {
        lockConfig.theta = (double)atof(argMap[THETA]);
      }

      this->ccType = LOCKING;
    } else if (ccType == OCC) {

      if (argMap.count(NUM_LOCK_THREADS) == 0 || 
          argMap.count(NUM_TXNS) == 0 ||
          argMap.count(NUM_RECORDS) == 0 ||
          argMap.count(NUM_CONTENDED) == 0 ||
          argMap.count(TXN_SIZE) == 0 || 
          argMap.count(EXPERIMENT) == 0 ||
          argMap.count(RECORD_SIZE) == 0 || 
          argMap.count(DISTRIBUTION) == 0 ||
          argMap.count(OCC_EPOCH) == 0) {
        
        std::cerr << "Missing one or more OCC params\n";
        std::cerr << "--" << long_options[NUM_LOCK_THREADS].name << "\n";
        std::cerr << "--" << long_options[NUM_TXNS].name << "\n";
        std::cerr << "--" << long_options[NUM_RECORDS].name << "\n";
        std::cerr << "--" << long_options[NUM_CONTENDED].name << "\n";
        std::cerr << "--" << long_options[TXN_SIZE].name << "\n";
        std::cerr << "--" << long_options[EXPERIMENT].name << "\n";
        std::cerr << "--" << long_options[RECORD_SIZE].name << "\n";
        std::cerr << "--" << long_options[DISTRIBUTION].name << "\n";
        std::cerr << "--" << long_options[OCC_EPOCH].name << "\n";
        exit(-1);
      }
      occConfig.numThreads = (uint32_t)atoi(argMap[NUM_LOCK_THREADS]);
      occConfig.numTxns = (uint32_t)atoi(argMap[NUM_TXNS]);
      occConfig.numRecords = (uint32_t)atoi(argMap[NUM_RECORDS]);
      occConfig.numContendedRecords = 
        (uint32_t)atoi(argMap[NUM_CONTENDED]);
      occConfig.txnSize = (uint32_t)atoi(argMap[TXN_SIZE]);
      occConfig.experiment = (uint32_t)atoi(argMap[EXPERIMENT]);
      occConfig.recordSize = (uint64_t)atoi(argMap[RECORD_SIZE]);
      occConfig.distribution = (uint32_t)atoi(argMap[DISTRIBUTION]);
      if (argMap.count(THETA) > 0) {
        occConfig.theta = (double)atof(argMap[THETA]);
      }
      occConfig.occ_epoch = (uint32_t)atoi(argMap[OCC_EPOCH]);              
      this->ccType = OCC;
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
