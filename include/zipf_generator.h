#ifndef         ZIPF_GENERATOR_H_
#define         ZIPF_GENERATOR_H_

#include <record_generator.h>
#include <iostream>
#include <cmath>
#include <pthread.h>
#include <stdint.h>
#include <time.h>

// Communicate zeta params to a thread. The thread writes its output to "ret"
struct ZetaParams {
  uint64_t start;
  uint64_t end;
  double theta;
  volatile double ret;
};

class ZipfGenerator : public RecordGenerator {

 private:
  double theta;
  uint64_t numElems;
  double zetan;  

  static double ZetaPartition(ZetaParams *zetaParams);
  static double GenZeta(uint64_t numThreads, uint64_t numElems, double theta);

 public:
  ZipfGenerator(uint64_t zetaThreads, uint64_t numElems, double theta);
  
  virtual uint64_t GenNext();
};

#endif          // ZIPF_GENERATOR_H_
