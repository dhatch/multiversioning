#ifndef SETUP_WORKLOAD_H_
#define SETUP_WORKLOAD_H_

#include <db.h>

struct workload_config;

txn* generate_transaction(workload_config conf);
uint32_t generate_input(workload_config conf, txn ***loaders);

#endif // SETUP_WORKLOAD_H_
