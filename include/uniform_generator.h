#ifndef         UNIFORM_GENERATOR_H_
#define         UNIFORM_GENERATOR_H_

#include <record_generator.h>

class UniformGenerator : public RecordGenerator {
 private:
  uint64_t numElems; 

 public:
  UniformGenerator(uint64_t numElems) {
    this->numElems = numElems;
  }

  virtual ~UniformGenerator() {
  }
  
  virtual uint64_t GenNext() {
    return (uint64_t)rand() % this->numElems;
  }
};

#endif          // UNIFORM_GENERATOR_H_
