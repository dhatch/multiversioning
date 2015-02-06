#ifndef SETUP_OCC_H_
#define SETUP_OCC_H_

#include <config.h>
#include <table.h>
#include <occ.h>
#include <record_generator.h>

OCCAction** create_single_occ_action_batch(uint32_t numTxns, uint32_t txnSize,
                                           uint64_t numRecords,
                                           uint32_t experiment,
                                           RecordGenerator *gen);

OCCAction* generate_occ_rmw_action(RecordGenerator *gen, uint32_t txnSize,
                                   int experiment);

OCCAction* generate_small_bank_occ_action(uint64_t numRecords, bool read_only);

OCCActionBatch* setup_occ_input(OCCConfig config);

OCCWorker** setup_occ_workers(SimpleQueue<OCCActionBatch> **inputQueue,
                              SimpleQueue<OCCActionBatch> **outputQueue,
                              Table **tables, int numThreads,
                              uint64_t epoch_threshold, uint32_t numTables);

void validate_ycsb_occ_tables(Table *table, uint64_t num_records);

Table** setup_ycsb_occ_tables(OCCConfig config);

Table** setup_small_bank_occ_tables(OCCConfig config);

Table** setup_occ_tables(OCCConfig config);

void write_occ_output(timespec elapsed_time, OCCConfig config);

timespec do_measurement(SimpleQueue<OCCActionBatch> **inputQueues,
                        SimpleQueue<OCCActionBatch> **outputQueues,
                        OCCWorker **workers, OCCActionBatch *inputBatches,
                        OCCConfig config);

timespec run_occ_workers(SimpleQueue<OCCActionBatch> **inputQueues,
                         SimpleQueue<OCCActionBatch> **outputQueues,
                         OCCWorker **workers, OCCActionBatch *inputBatches,
                         OCCConfig config);

void occ_experiment(OCCConfig config);

#endif // SETUP_OCC_H_




        
        

