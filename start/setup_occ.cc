#include <setup_occ.h>
#include <config.h>
#include <common.h>
#include <set>
#include <small_bank.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <gperftools/profiler.h>
#include <fstream>

OCCAction** create_single_occ_action_batch(uint32_t batch_size,
                                           OCCConfig config,
                                           RecordGenerator *gen)
{
        uint32_t i;
        OCCAction **ret;
        ret = (OCCAction**)alloc_mem(batch_size*sizeof(OCCAction*), 71);
        assert(ret != NULL);
        memset(ret, 0x0, batch_size*sizeof(OCCAction*));
        for (i = 0; i < batch_size; ++i) {
                if (config.experiment == 3)
                        ret[i] = generate_small_bank_occ_action(config.numRecords,
                                                                false);
                else if (config.experiment == 4)
                        ret[i] = generate_small_bank_occ_action(config.numRecords,
                                                                true);
                else if (config.experiment < 3) 
                        ret[i] = generate_occ_rmw_action(config, gen);
        }
        return ret;
}

OCCAction* generate_occ_rmw_action(OCCConfig config, RecordGenerator *gen)
{
        OCCAction *action = new RMWOCCAction();
        std::set<uint64_t> seen_keys;
        int flip = rand() % 100;
        for (uint32_t j = 0; j < config.txnSize; ++j) {
                uint64_t key = GenUniqueKey(gen, &seen_keys);
                if (config.experiment == 0) {
                        if (flip < 50) {
                                action->AddReadKey(0, key, false);
                        } else if (j < 5) {
                                action->AddReadKey(0, key, true);
                                action->AddWriteKey(0, key);
                        }
                } else if (config.experiment == 1) {
                        if (flip < 1) {
                                action->AddReadKey(0, key, false);
                        } else {
                                if (j < 2) {
                                        action->AddReadKey(0, key, true);
                                        action->AddWriteKey(0, key);
                                } else {
                                        action->AddReadKey(0, key, false);
                                }
                        }
                } else if (config.experiment == 2) {
                        if (flip == 0) {
                                action->AddReadKey(0, key, false);
                        } else {
                                if (j < 2) {
                                        action->AddWriteKey(0, key);
                                } else {
                                        action->AddReadKey(0, key, false);
                                }
                        }
                }
        }
        return action;
}

OCCAction* generate_small_bank_occ_action(uint64_t numRecords, bool read_only)
{        
        OCCAction *ret = NULL;
        char *temp_buf;
        int mod, txn_type;
        long amount;
        uint64_t customer, from_customer, to_customer;
        if (read_only == true) 
                mod = 1;
        else 
                mod = 5;        
        temp_buf = (char*)malloc(METADATA_SIZE);        
        GenRandomSmallBank(temp_buf);
        txn_type = rand() % mod;
        if (txn_type == 0) {             // Balance
                customer = (uint64_t)(rand() % numRecords);
                ret = new OCCSmallBank::Balance(customer, temp_buf);
        } else if (txn_type == 1) {        // DepositChecking
                customer = (uint64_t)(rand() % numRecords);
                amount = (long)(rand() % 25);
                ret = new OCCSmallBank::DepositChecking(customer, amount,
                                                        temp_buf);
        } else if (txn_type == 2) {        // TransactSaving
                customer = (uint64_t)(rand() % numRecords);
                amount = (long)(rand() % 25);
                ret = new OCCSmallBank::TransactSaving(customer, amount,
                                                       temp_buf);
        } else if (txn_type == 3) {        // Amalgamate
                from_customer = (uint64_t)(rand() % numRecords);
                to_customer;
                do {
                        to_customer = (uint64_t)(rand() % numRecords);
                } while (to_customer == from_customer);
                ret = new OCCSmallBank::Amalgamate(from_customer, to_customer,
                                                   temp_buf);
        } else if (txn_type == 4) {        // WriteCheck
                customer = (uint64_t)(rand() % numRecords);
                amount = (long)(rand() % 25);
                if (rand() % 2 == 0) {
                        amount *= -1;
                }
                ret = new OCCSmallBank::WriteCheck(customer, amount, temp_buf);
        }
        return ret;
}

OCCActionBatch** setup_occ_input(OCCConfig config, uint32_t iters)
{
        OCCActionBatch **ret;
        uint32_t i;
        ret = (OCCActionBatch**)malloc(sizeof(OCCActionBatch*)*iters);
        for (i = 0; i < iters; ++i) 
                ret[i] = setup_occ_single_input(config);
        return ret;
}

OCCActionBatch* setup_occ_single_input(OCCConfig config)
{
        RecordGenerator *gen = NULL;
        OCCActionBatch *ret;
        uint32_t txns_per_thread, remainder, i, j;
        OCCAction **actions;
        if (config.distribution == 0) 
                gen = new UniformGenerator(config.numRecords);
        else if (config.distribution == 1) 
                gen = new ZipfGenerator(0, config.numRecords, config.theta);
        ret = (OCCActionBatch*)malloc(sizeof(OCCActionBatch)*config.numThreads);
        txns_per_thread = (config.numTxns)/config.numThreads;
        remainder = (config.numTxns) % config.numThreads;
        for (i = 0; i < config.numThreads; ++i) {
                if (i == config.numThreads-1)
                        txns_per_thread += remainder;
                actions = create_single_occ_action_batch(txns_per_thread,
                                                         config,
                                                         gen);
                ret[i] = {
                        txns_per_thread,
                        actions,
                };
        }
        return ret;
}

OCCWorker** setup_occ_workers(SimpleQueue<OCCActionBatch> **inputQueue,
                              SimpleQueue<OCCActionBatch> **outputQueue,
                              Table **tables, int numThreads,
                              uint64_t epoch_threshold, uint32_t numTables)
{
        uint32_t recordSizes[2];
        OCCWorker **workers;
        volatile uint32_t *epoch_ptr;
        int i;
        bool is_leader;
        struct OCCWorkerConfig worker_config;
        struct RecordBuffersConfig buf_config;
        recordSizes[0] = recordSize;
        recordSizes[1] = recordSize;
        workers = (OCCWorker**)malloc(sizeof(OCCWorker*)*numThreads);
        assert(workers != NULL);
        epoch_ptr = (volatile uint32_t*)alloc_mem(sizeof(uint32_t), 0);
        assert(epoch_ptr != NULL);
        barrier();
        *epoch_ptr = 0;
        barrier();
        for (i = 0; i < numThreads; ++i) {
                is_leader = (i == 0);
                worker_config = {
                        inputQueue[i],
                        outputQueue[i],
                        i,
                        tables,
                        is_leader,
                        epoch_ptr,
                        epoch_threshold,
                        false,
                };
                buf_config = {
                        numTables,
                        recordSizes,
                        100,
                        i,
                };                
                workers[i] = new OCCWorker(worker_config, buf_config);
        }
        return workers;
}

void validate_ycsb_occ_tables(Table *table, uint64_t num_records)
{
        uint64_t *temp;
        for (uint64_t i = 0; i < num_records; ++i) {
                temp = (uint64_t*)table->Get(i);
                assert(temp[0] == 0);
                if (recordSize == 8)
                        assert(temp[1] == i);
                else
                        temp[125] += 1;
        } 
}

Table** setup_ycsb_occ_tables(OCCConfig config)
{
        TableConfig tbl_config;
        Table **tables;
        uint64_t *big_int, i, j;
        char buf[OCC_RECORD_SIZE(1000)];
        assert(config.experiment < 3);
        tbl_config = create_table_config(0, config.numRecords, 0,
                                         config.numThreads-1, config.numRecords,
                                         OCC_RECORD_SIZE(recordSize));
        tables = (Table**)malloc(sizeof(Table*));
        tables[0] = new(0) Table(tbl_config);
        for ( i = 0; i < config.numRecords; ++i) {
                big_int = (uint64_t*)buf;
                if (recordSize == 1000) {
                        assert(OCC_RECORD_SIZE(recordSize) == 1008);
                        for (j = 0; j < 125; ++j) 
                                big_int[j+1] = (uint64_t)rand();
                        big_int[0] = 0;
                        tables[0]->Put(i, buf);
                } else if (recordSize == 8) {
                        assert(OCC_RECORD_SIZE(recordSize) == 16);
                        big_int[0] = 0;
                        big_int[1] = i;
                        tables[0]->Put(i, buf);
                }
        }
        tables[0]->SetInit();
        validate_ycsb_occ_tables(tables[0], config.numRecords);
        return tables;
}

Table** setup_small_bank_occ_tables(OCCConfig config)
{
        assert(config.experiment == 3 || config.experiment == 4);
        Table **tables;
        uint64_t *table_sizes;
        uint32_t num_tables, i;
        TableConfig savings_config, checking_config;
        char temp[OCC_RECORD_SIZE(sizeof(SmallBankRecord))];
        SmallBankRecord *record;
        table_sizes = (uint64_t*)malloc(2*sizeof(uint64_t));
        table_sizes[0] = (uint64_t)config.numRecords;
        table_sizes[1] = (uint64_t)config.numRecords;
        num_tables = 2;
        savings_config =
                create_table_config(SAVINGS, 1000000, 0,
                                    (int)(config.numThreads-1), 1000000,
                                    OCC_RECORD_SIZE(sizeof(SmallBankRecord)));
        checking_config =
                create_table_config(CHECKING, 1000000, 0,
                                    (int)(config.numThreads-1), 1000000,
                                    OCC_RECORD_SIZE(sizeof(SmallBankRecord)));
        tables = (Table**)malloc(sizeof(Table*)*2);    
        tables[SAVINGS] = new(0) Table(savings_config);
        tables[CHECKING] = new(0) Table(checking_config);
        memset(temp, 0, OCC_RECORD_SIZE(sizeof(SmallBankRecord)));
        for (i = 0; i < 1000000; ++i) {
                record = (SmallBankRecord*)&temp[sizeof(uint64_t)];
                record->amount = (long)(rand() % 100);
                tables[SAVINGS]->Put((uint64_t)i, temp);
                record->amount = (long)(rand() % 100);
                tables[CHECKING]->Put((uint64_t)i, temp);
        }
        tables[SAVINGS]->SetInit();
        tables[CHECKING]->SetInit();
        return tables;
}

Table** setup_occ_tables(OCCConfig config)
{
        Table **tables = NULL;
        if (config.experiment < 3) {
                tables = setup_ycsb_occ_tables(config);
        } else if (config.experiment == 3) {
                tables = setup_small_bank_occ_tables(config);
        }
        return tables;
}

void write_occ_output(struct occ_result result, OCCConfig config)
{
        double elapsed_milli;
        timespec elapsed_time;
        std::ofstream result_file;
        elapsed_time = result.time_elapsed;
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        std::cout << elapsed_milli << '\n';
        result_file.open("occ.txt", std::ios::app | std::ios::out);
        result_file << "time:" << elapsed_milli << " txns:" << result.num_txns;
        result_file << " threads:" << config.numThreads << " occ ";
        result_file << "records:" << config.numRecords << " ";
        if (config.experiment == 0) 
                result_file << "10rmw" << " ";
        else if (config.experiment == 1)
                result_file << "8r2rmw" << " ";
        else if (config.experiment == 2)
                result_file << "2r8w" << " ";
        else if (config.experiment == 3) 
                result_file << "small_bank" << " "; 
        if (config.distribution == 0) 
                result_file << "uniform" << "\n";        
        else if (config.distribution == 1) 
                result_file << "zipf theta:" << config.theta << "\n";
        result_file.close();  
}

uint64_t get_completed_count(OCCWorker **workers, uint32_t num_workers,
                             OCCActionBatch *input_batches)
{
        volatile uint64_t cur;
        uint64_t total_completed = 0;
        uint32_t i;
        for (i = 0; i < num_workers; ++i) {
                cur = workers[i]->NumCompleted();
                if (cur >= (input_batches[i].batchSize)) {
                        total_completed += cur;
                } else {
                        total_completed = 0;
                        break;
                }
        }
        return total_completed;
}

uint64_t wait_to_completion(SimpleQueue<OCCActionBatch> **output_queues,
                            OCCWorker **workers, uint32_t num_workers,
                            uint64_t threshold, OCCActionBatch *input_batches)
{
        uint32_t i;
        volatile uint64_t num_completed = 0;
        OCCActionBatch done;
        volatile uint64_t start, end;
        /*
        barrier();
        start = rdtsc();
        barrier();
        
        while (true) {                
                for (i = 0; i < OCC_WAIT_INTERVAL; ++i) {
                        single_work();
                }
                end = rdtsc();
                if (end - start > 2*1000000000)
                        break;
        }
        barrier();
        */
        for (i = 0; i < num_workers; ++i) {
                num_completed += workers[i]->NumCompleted();
        }
        //        barrier();
        return num_completed;
}

struct occ_result do_measurement(SimpleQueue<OCCActionBatch> **inputQueues,
                                 SimpleQueue<OCCActionBatch> **outputQueues,
                                 OCCWorker **workers,
                                 OCCActionBatch **inputBatches,
                                 uint32_t num_batches,
                                 OCCConfig config)
{
        timespec start_time, end_time, elapsed_time;
        uint32_t i, j;
        uint64_t num_completed;
        struct occ_result result;
        for (i = 0; i < config.numThreads; ++i)
                workers[i]->Run();
        if (PROFILE)
                ProfilerStart("occ.prof");
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        barrier();
        for (i = 0; i < num_batches; ++i) 
                for (j = 0; j < config.numThreads; ++j) 
                        inputQueues[j]->EnqueueBlocking(inputBatches[i][j]);
        barrier();
        result.num_txns = wait_to_completion(outputQueues, workers,
                                             config.numThreads, config.numTxns,
                                             inputBatches[0]);
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        barrier();
        std::cout << "Num completed: " << result.num_txns << "\n";
        if (PROFILE)
                ProfilerStop();
        result.time_elapsed = diff_time(end_time, start_time);
        return result;
}

struct occ_result run_occ_workers(SimpleQueue<OCCActionBatch> **inputQueues,
                                  SimpleQueue<OCCActionBatch> **outputQueues,
                                  OCCWorker **workers,
                                  OCCActionBatch **inputBatches,
                                  uint32_t num_batches,
                                  OCCConfig config)
{
        int success;
        struct occ_result result;        
        success = pin_thread(79);
        assert(success == 0);
        result = do_measurement(inputQueues, outputQueues, workers,
                                inputBatches, num_batches, config);
        return result;
}

void occ_experiment(OCCConfig config)
{
        SimpleQueue<OCCActionBatch> **input_queues, **output_queues;
        Table **tables;
        OCCWorker **workers;
        OCCActionBatch **inputs;
        struct occ_result result;
        input_queues = setup_queues<OCCActionBatch>(config.numThreads, 1024);
        output_queues = setup_queues<OCCActionBatch>(config.numThreads, 1024);
        tables = setup_occ_tables(config);
        workers = setup_occ_workers(input_queues, output_queues, tables,
                                    config.numThreads, config.occ_epoch, 2);
        inputs = setup_occ_input(config, OCC_TXN_BUFFER);
        result = run_occ_workers(input_queues, output_queues, workers,
                                 inputs, OCC_TXN_BUFFER, config);
        write_occ_output(result, config);
}
