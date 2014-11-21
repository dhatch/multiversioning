#ifndef         RECORD_GENERATOR_H_
#define         RECORD_GENERATOR_H_

#include <stdint.h>

class RecordGenerator {
 public:
  virtual uint64_t GenNext() = 0;  
};

#endif          // RECORD_GENERATOR_H_
