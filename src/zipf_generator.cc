#include <zipf_generator.h>
#include <cassert>

// Each thread computes the contribution of a specific range of elements to zeta
double ZipfGenerator::ZetaPartition(ZetaParams* zetaParams) {
  double sum = 0;
  for (uint64_t i = zetaParams->start; i <= zetaParams->end; ++i) {
    sum += 1.0 / pow((double)i, zetaParams->theta);
  }
  return sum;
}

double ZipfGenerator::GenZeta(uint64_t numElems, double theta)
{
  ZetaParams params;
  params.start = 1;
  params.end = numElems;
  params.theta = theta;
  return ZetaPartition(&params);
}

ZipfGenerator::ZipfGenerator(uint64_t numElems, double theta) {
  this->theta = theta;
  this->numElems = numElems;
  this->zetan = GenZeta(numElems, theta);  
}

uint64_t ZipfGenerator::GenNext() {
  double alpha = 1 / (1 - this->theta);
  double eta = (1 - pow(2.0 / this->numElems, 1 - this->theta));
  double u = (double)rand() / ((double)RAND_MAX);
  double uz = u * this->zetan;
  if (uz < 1.0) {
    return 0;
  }
  else if (uz < (1.0 + pow(0.5, this->theta))) {
    return 1;
  }
  else {
    uint64_t temp = (uint64_t)(this->numElems*pow(eta*u - eta + 1, alpha));
    assert(temp > 0 && temp <= this->numElems);
    return temp-1;
  }
}

ZipfGenerator::~ZipfGenerator()
{
}
      
