#include <setup_occ.h>
#include <config.h>
#include <common.h>
#include <set>
#include <small_bank.h>
#include <fstream>
#include <setup_workload.h>

extern uint32_t GLOBAL_RECORD_SIZE;

OCCAction* setup_occ_action(txn *txn)
{
        OCCAction *action;
        struct big_key *array;
        uint32_t num_reads, num_writes, num_rmws, max, i;
        
        action = new OCCAction(txn);
        txn->set_translator(action);
        num_reads = txn->num_reads();
        num_writes = txn->num_writes();
        num_rmws = txn->num_rmws();

        if (num_reads >= num_writes && num_reads >= num_rmws) 
                max = num_reads;
        else if (num_writes >= num_rmws)
                max = num_writes;
        else
                max = num_rmws;
        array = (struct big_key*)malloc(sizeof(struct big_key)*max);

        txn->get_reads(array);
        for (i = 0; i < num_reads; ++i) 
                action->add_read_key(array[i].table_id, array[i].key);
        txn->get_rmws(array);
        for (i = 0; i < num_rmws; ++i) 
                action->add_write_key(array[i].table_id, array[i].key, true);
        txn->get_writes(array);
        for (i = 0; i < num_writes; ++i) {
                action->add_write_key(array[i].table_id, array[i].key, false);
        }        
        free(array);
        return action;
}

OCCAction** create_single_occ_action_batch(uint32_t batch_size,
                                           workload_config w_config)
{
        uint32_t i;
        OCCAction **ret;
        txn *txn;
        
        ret = (OCCAction**)alloc_mem(batch_size*sizeof(OCCAction*), 71);
        assert(ret != NULL);
        memset(ret, 0x0, batch_size*sizeof(OCCAction*));
        for (i = 0; i < batch_size; ++i) {
                txn = generate_transaction(w_config);
                ret[i] = setup_occ_action(txn);
        }
        return ret;
}

OCCActionBatch** setup_occ_input(OCCConfig occ_config, workload_config w_conf,
                                 uint32_t extra_batches)
{
        OCCActionBatch **ret;
        uint32_t i, total_iters;
        OCCConfig fake_config;
        
        total_iters = 1 + 1 + extra_batches; // dry run + real run + extra
        fake_config = occ_config;
        fake_config.numTxns = FAKE_ITER_SIZE;
        ret = (OCCActionBatch**)malloc(sizeof(OCCActionBatch*)*(total_iters));
        ret[0] = setup_occ_single_input(fake_config, w_conf);
        ret[1] = setup_occ_single_input(occ_config, w_conf);
        occ_config.numTxns = 1000000;
        fake_config.numTxns = occ_config.numTxns;
        for (i = 2; i < total_iters; ++i) 
                ret[i] = setup_occ_single_input(fake_config, w_conf);
        std::cerr << "Done setting up occ input\n";
        return ret;
}

OCCActionBatch* setup_occ_single_input(OCCConfig config, workload_config w_conf)
{
        OCCActionBatch *ret;
        uint32_t txns_per_thread, remainder, i;
        OCCAction **actions;

        config.numThreads -= 1;
        ret = (OCCActionBatch*)malloc(sizeof(OCCActionBatch)*config.numThreads);
        txns_per_thread = (config.numTxns)/config.numThreads;
        remainder = (config.numTxns) % config.numThreads;
        for (i = 0; i < config.numThreads; ++i) {
                if (i == config.numThreads-1)
                        txns_per_thread += remainder;
                actions = create_single_occ_action_batch(txns_per_thread,
                                                         w_conf);
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
        recordSizes[0] = GLOBAL_RECORD_SIZE;
        recordSizes[1] = GLOBAL_RECORD_SIZE;
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
                        0,
                        epoch_threshold,
                        OCC_LOG_SIZE,
                        false,
                        numTables,
                };
                buf_config = {
                        numTables,
                        recordSizes,
                        5000,
                        i,
                };                
                workers[i] = new(i) OCCWorker(worker_config, buf_config);
        }
        std::cerr << "Done setting up occ workers\n";
        return workers;
}

Table** setup_hash_tables(uint32_t num_tables, uint32_t *num_records, bool occ)
{
        Table **tables;
        uint32_t i;
        TableConfig conf;

        tables = (Table**)malloc(sizeof(Table*)*num_tables);
        for (i = 0; i < num_tables; ++i) {
                conf.tableId = i;
                conf.numBuckets = (uint64_t)num_records[i];
                conf.startCpu = 0;
                conf.endCpu = 71;
                conf.freeListSz = 2*num_records[i];
                if (occ)
                        conf.valueSz = GLOBAL_RECORD_SIZE + 8;
                else
                        conf.valueSz = GLOBAL_RECORD_SIZE;
                conf.recordSize = 0;
                tables[i] = new(0) Table(conf);
        }
        return tables;
}

static OCCActionBatch setup_db(workload_config conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        OCCActionBatch ret;

        loader_txns = NULL;
        num_txns = generate_input(conf, &loader_txns);
        assert(loader_txns != NULL);
        ret.batchSize = num_txns;
        ret.batch = (OCCAction**)malloc(sizeof(mv_action*)*num_txns);
        for (i = 0; i < num_txns; ++i) 
                ret.batch[i] = setup_occ_action(loader_txns[i]);
        return ret;
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
        result_file << "read_pct:" << config.read_pct << " ";
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
                            uint32_t num_workers)
{        
        uint32_t i;
        uint64_t num_completed = 0;
        OCCActionBatch temp;
        for (i = 1; i < num_workers; ++i) {
                temp = output_queues[i]->DequeueBlocking();
                num_completed += temp.batchSize;
        }
        return num_completed;
}

void populate_tables(SimpleQueue<OCCActionBatch> *input_queue,
                    SimpleQueue<OCCActionBatch> *output_queue,
                    OCCActionBatch input,
                    Table **tables,
                    uint32_t num_tables)
{
        uint32_t i;
        
        input_queue->EnqueueBlocking(input);
        barrier();
        output_queue->DequeueBlocking();
        for (i = 0; i < num_tables; ++i)
                tables[i]->SetInit();
        
}

void dry_run(SimpleQueue<OCCActionBatch> **input_queues, 
             SimpleQueue<OCCActionBatch> **output_queues,
             OCCActionBatch *input_batches,
             uint32_t num_workers)
{
        uint32_t i;
        for (i = 1; i < num_workers; ++i) 
                input_queues[i]->EnqueueBlocking(input_batches[i-1]);
        barrier();
        for (i = 1; i < num_workers; ++i) 
                output_queues[i]->DequeueBlocking();
}

struct occ_result do_measurement(SimpleQueue<OCCActionBatch> **inputQueues,
                                 SimpleQueue<OCCActionBatch> **outputQueues,
                                 OCCWorker **workers,
                                 OCCActionBatch **inputBatches,
                                 uint32_t num_batches,
                                 OCCConfig config,
                                 OCCActionBatch setup_txns,
                                 Table **tables,
                                 uint32_t num_tables)
{
        timespec start_time, end_time;
        uint32_t i, j;
        struct occ_result result;
                std::cerr << "Num batches " << num_batches << "\n";
        for (i = 0; i < config.numThreads; ++i) {
                workers[i]->Run();
                workers[i]->WaitInit();
        }

        populate_tables(inputQueues[1], outputQueues[1], setup_txns, tables,
                        num_tables);
        dry_run(inputQueues, outputQueues, inputBatches[0], config.numThreads);

        std::cerr << "Done dry run\n";
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        barrier();
        for (i = 0; i < num_batches; ++i) 
                for (j = 0; j < config.numThreads-1; ++j) 
                        inputQueues[j+1]->EnqueueBlocking(inputBatches[i+1][j]);
        barrier();
        result.num_txns = wait_to_completion(outputQueues, config.numThreads);
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        barrier();
        result.time_elapsed = diff_time(end_time, start_time);
        //        result.num_txns = config.numTxns;
        std::cout << "Num completed: " << result.num_txns << "\n";
        return result;
}

struct occ_result run_occ_workers(SimpleQueue<OCCActionBatch> **inputQueues,
                                  SimpleQueue<OCCActionBatch> **outputQueues,
                                  OCCWorker **workers,
                                  OCCActionBatch **inputBatches,
                                  uint32_t num_batches,
                                  OCCConfig config, OCCActionBatch setup_txns,
                                  Table **tables, uint32_t num_tables)
{
        int success;
        struct occ_result result;        

        success = pin_thread(79);
        assert(success == 0);
        result = do_measurement(inputQueues, outputQueues, workers,
                                inputBatches, num_batches, config, setup_txns,
                                tables, num_tables);
        std::cerr << "Done experiment!\n";
        return result;
}

void occ_experiment(OCCConfig occ_config, workload_config w_conf)
{
        SimpleQueue<OCCActionBatch> **input_queues, **output_queues;
        Table **tables;
        OCCWorker **workers;
        OCCActionBatch **inputs;
        OCCActionBatch setup_txns;
        
        struct occ_result result;
        uint32_t num_records[2];
        uint32_t num_tables;
        
	occ_config.occ_epoch = OCC_EPOCH_SIZE;
        input_queues = setup_queues<OCCActionBatch>(occ_config.numThreads,
                                                    1024);
        output_queues = setup_queues<OCCActionBatch>(occ_config.numThreads,
                                                     1024);
        setup_txns = setup_db(w_conf);
        if (occ_config.experiment < 3) {
                num_tables = 1;
                num_records[0] = occ_config.numRecords;
        } else if (occ_config.experiment < 5) {
                num_tables = 2;
                num_records[0] = occ_config.numRecords;
                num_records[1] = occ_config.numRecords;
        } else {
                assert(false);
                tables = NULL;
                num_tables = 0;
        }
        tables = setup_hash_tables(num_tables, num_records, true);
        workers = setup_occ_workers(input_queues, output_queues, tables,
                                    occ_config.numThreads, occ_config.occ_epoch,
                                    2);

        inputs = setup_occ_input(occ_config, w_conf, 1);
        pin_memory();
        result = run_occ_workers(input_queues, output_queues, workers,
                                 inputs, 1+1, occ_config,
                                 setup_txns, tables, num_tables);
        write_occ_output(result, occ_config);
}
