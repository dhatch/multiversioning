#include <setup_workload.h>
#include <common.h>
#include <db.h>
#include <locking_action.h>
#include <lock_manager.h>
#include <eager_worker.h>
#include <table.h>
#include <config.h>
#include <fstream>
#include <sys/time.h>

#define EXTRA_BATCHES 1

extern uint32_t GLOBAL_RECORD_SIZE;
uint32_t record_sizes[2];

struct locking_result {
        double time;
        timespec elapsed_time;
        uint64_t num_txns;
};

static inline double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec/1e6;
}


static locking_action* txn_to_action(txn *t)
{
        locking_action *ret;
        struct big_key *arr;
        uint32_t i, num_reads, num_writes, num_rmws;

        arr = setup_array(t);
        ret = new locking_action(t);
        t->set_translator(ret);
        
        num_reads = t->num_reads();
        t->get_reads(arr);
        for (i = 0; i < num_reads; ++i) 
                ret->add_read_key(arr[i].key, arr[i].table_id);

        num_rmws = t->num_rmws();
        t->get_rmws(arr);
        for (i = 0; i < num_rmws; ++i) 
                ret->add_write_key(arr[i].key, arr[i].table_id);

        num_writes = t->num_writes();
        t->get_writes(arr);
        for (i = 0; i < num_writes; ++i) 
                ret->add_write_key(arr[i].key, arr[i].table_id);
        ret->prepare();
        return ret;
}

static locking_action* generate_action(workload_config w_conf)
{
        locking_action *ret;
        txn *t;

        t = generate_transaction(w_conf);
        assert(t != NULL);
        ret = txn_to_action(t);
        assert(ret != NULL);
        return ret;
}

static locking_action_batch create_single_batch(uint32_t num_txns,
                                                workload_config w_conf)
{
        locking_action_batch ret;
        uint32_t i;
        
        ret.batchSize = num_txns;
        ret.batch = (locking_action**)malloc(sizeof(locking_action*)*num_txns);
        for (i = 0; i < num_txns; ++i)
                ret.batch[i] = generate_action(w_conf);        
        return ret;
}

static locking_action_batch* setup_single_round(uint32_t num_txns,
                                                uint32_t num_threads,
                                                workload_config w_conf)
{
        locking_action_batch *ret;
        uint32_t i, txns_per_thread, remainder;

        ret = (locking_action_batch*)malloc(sizeof(locking_action_batch)*
                                            num_threads);
        txns_per_thread = num_txns / num_threads;
        remainder = num_txns % num_threads;
        for (i = 0; i < num_threads; ++i) {
                if (i < remainder)
                        ret[i] = create_single_batch(txns_per_thread+1, w_conf);
                else
                        ret[i] = create_single_batch(txns_per_thread, w_conf);
        }
        return ret;
}

static locking_action_batch** setup_input(locking_config conf,
                                          workload_config w_conf,
                                          uint32_t extra_batches)
{
        locking_action_batch **ret;
        uint32_t i, total_iters;
        
        /* dry run (1) + real run (1) + extra runs (extra_batches) */
        total_iters = 1 + 1 + extra_batches;
        ret = (locking_action_batch**)
                malloc(sizeof(locking_action_batch*)*total_iters);
        ret[0] = setup_single_round(FAKE_ITER_SIZE*conf.num_threads, conf.num_threads, w_conf);
        ret[1] = setup_single_round(conf.num_txns, conf.num_threads, w_conf);
        for (i = 2; i < total_iters; ++i)
                ret[i] = setup_single_round(conf.num_txns, conf.num_threads,
                                            w_conf);
        return ret;
}

static locking_worker** setup_workers(locking_queue **input,
                                      locking_queue **output,
                                      LockManager *mgr,
                                      uint32_t num_threads,
                                      uint32_t num_pending,
                                      Table **tables,
                                      uint32_t num_tables)
{
        assert(mgr != NULL && tables != NULL);
        locking_worker **ret;
        int i;

        record_sizes[0] = GLOBAL_RECORD_SIZE;
        record_sizes[1] = GLOBAL_RECORD_SIZE;
        ret = (locking_worker**)malloc(sizeof(locking_worker*)*num_threads);
        assert(ret != NULL);
        for (i = 0; i < num_threads; ++i) {
                struct locking_worker_config conf = {
                        mgr,
                        input[i],
                        output[i],
                        i,
                        num_pending,
                        tables,                        
                };
                struct RecordBuffersConfig rb_conf = {
                        num_tables,
                        record_sizes,
                        5000,
                        i,
                };

                ret[i] = new(i) locking_worker(conf, rb_conf);
        }
        return ret;
}
                                      
static locking_action_batch setup_db(workload_config w_conf)
{
        txn **loader_txns;
        uint32_t num_txns, i;
        locking_action_batch ret;

        loader_txns = NULL;
        num_txns = generate_input(w_conf, &loader_txns);
        assert(loader_txns != NULL);
        ret.batchSize = num_txns;
        ret.batch = (locking_action**)malloc(sizeof(locking_action*)*num_txns);
        assert(ret.batch != NULL);
        for (i = 0; i < num_txns; ++i) 
                ret.batch[i] = txn_to_action(loader_txns[i]);
        return ret;
}

static void write_locking_output(locking_config conf,
                                 struct locking_result result)
{
        std::ofstream result_file;
        double elapsed_milli;
        timespec elapsed_time;

        elapsed_time = result.elapsed_time;
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        result_file.open("locking.txt", std::ios::app | std::ios::out);
        result_file << "locking ";
        result_file << "time:" << elapsed_milli << " ";
        result_file << "txns:" << conf.num_txns << " ";
        result_file << "threads:" << conf.num_threads << " ";
        result_file << "records:" << conf.num_records << " ";
        result_file << "read_pct:" << conf.read_pct << " ";
        if (conf.experiment == 0) 
                result_file << "10rmw" << " ";
        else if (conf.experiment == 1) 
                result_file << "8r2rmw" << " ";
        else if (conf.experiment == 3) 
                result_file << "small_bank" << " ";
 
        if (conf.distribution == 0) 
                result_file << "uniform ";
        else if (conf.distribution == 1) 
                result_file << "zipf theta:" << conf.theta << " ";
        else
                assert(false);

        result_file << "\n";
        result_file.close();  
        std::cout << "Time elapsed: " << result.time << " ";
        std::cout << "Num txns: " << conf.num_txns << "\n";
}

static struct locking_result do_measurement(locking_config conf,
                                            locking_worker **workers,
                                            locking_queue **inputs,
                                            locking_queue **outputs,
                                            locking_action_batch **batches,
                                            uint32_t num_batches,
                                            locking_action_batch setup,
                                            Table **tables,
                                            uint32_t num_tables)
{
        uint32_t i, j;
        struct locking_result result;
        timespec start_time, end_time;

        pin_thread(79);
        
        /* Start worker threads. */
        for (i = 0; i < conf.num_threads; ++i) {
                workers[i]->Run();
                workers[i]->WaitInit();
        }

        /* Setup the database. */
        inputs[0]->EnqueueBlocking(setup);
        outputs[0]->DequeueBlocking();
        for (i = 0; i < num_tables; ++i)
                tables[i]->SetInit();

        std::cerr << "Done setting up tables!\n";
        
        /* Dry run */
        for (i = 0; i < conf.num_threads; ++i) {
                inputs[i]->EnqueueBlocking(batches[0][i]);
        }
        for (i = 0; i < conf.num_threads; ++i) {
                outputs[i]->DequeueBlocking();
        }

        std::cerr << "Done with dry run!\n";
        
        double start_dbl = GetTime();
        barrier();
        clock_gettime(CLOCK_REALTIME, &start_time);
        barrier();

        for (i = 1; i < num_batches; ++i)
                for (j = 0; j < conf.num_threads; ++j)
                        inputs[j]->EnqueueBlocking(batches[i][j]);
        for (i = 0; i < conf.num_threads; ++i)
                outputs[i]->DequeueBlocking();
        double end_dbl = GetTime();
        barrier();
        clock_gettime(CLOCK_REALTIME, &end_time);
        barrier();
        result.time = end_dbl - start_dbl;
        result.elapsed_time = diff_time(end_time, start_time);
        return result;
}

void locking_experiment(locking_config conf, workload_config w_conf)
{
        locking_queue **inputs, **outputs;
        locking_action_batch **experiment_txns, setup_txns;
        Table **tables;
        uint32_t num_records[2], num_tables;
        struct LockManagerConfig mgr_config;
        struct locking_result result;
        LockManager *lock_manager;
        locking_worker **workers;
        
        inputs = setup_queues<locking_action_batch>(conf.num_threads, 1024);
        outputs = setup_queues<locking_action_batch>(conf.num_threads, 1024);
        setup_txns = setup_db(w_conf);
        experiment_txns = setup_input(conf, w_conf, EXTRA_BATCHES);
        
        if (w_conf.experiment < 3) {
                num_records[0] = w_conf.num_records;
                num_tables = 1;
        } else if (w_conf.experiment < 5) {
                num_tables = 2;
                num_records[0] = w_conf.num_records;
                num_records[1] = w_conf.num_records;
        } else {
                assert(false);
        }

        assert(conf.num_threads > 0);
        mgr_config = {
                num_tables,
                num_records,
                0,
                (int)conf.num_threads - 1,
        };
        tables = setup_hash_tables(num_tables, num_records, false);
        lock_manager = new LockManager(mgr_config);        
        workers = setup_workers(inputs, outputs, lock_manager,
                                conf.num_threads, 50, tables, num_tables);
        result = do_measurement(conf, workers, inputs, outputs, experiment_txns,
                                1+EXTRA_BATCHES, setup_txns, tables,
                                num_tables);
        write_locking_output(conf, result);
}
