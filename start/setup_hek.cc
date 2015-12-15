#include <hek.h>
#include <hek_table.h>
#include <hek_action.h>
#include <config.h>
#include <common.h>
#include <zipf_generator.h>
#include <uniform_generator.h>
#include <fstream>
#include <stdlib.h>
#include <algorithm>
#include <small_bank.h>

/* Total space available for free lists */
#define TOTAL_SIZE (((uint64_t)1) << 35)

struct hek_result {
        struct timespec elapsed_time;
        uint32_t num_txns;
};

/* 
 * Arrays initialized in the scope of this file. Passed to worker threads as 
 * read-only config info. 
 */
static uint64_t freelist_sizes[2];
static uint32_t record_sizes[2];


// 
// static bool key_cmp(hek_key key1, hek_key key2)
// {
//         if (key1.table_id < key2.table_id) 
//                 return true;
//         else if (key1.table_id > key2.table_id)
//                 return false;
//         else
//                 return key1.key < key2.key;
// }
// 

/*
 * Insert records into the YCSB table. 
 */
static void init_ycsb(hek_config config, hek_table *table)
{
        uint32_t i, record_size;
        char *records;
        hek_record *rec_ptr;
        record_size = 1000 + sizeof(hek_record);
        records = (char*)alloc_interleaved_all(record_size*config.num_records);
        memset(records, 0x0, record_size*config.num_records);
        for (i = 0; i < config.num_records; ++i) {
                rec_ptr = (hek_record*)(records + i*record_size);
                rec_ptr->next = NULL;
                rec_ptr->begin = 0;		/* "Created" at time 0 */
                rec_ptr->end = HEK_INF;		 
                rec_ptr->key = i;
                rec_ptr->size = 1000;
                GenRandomSmallBank(rec_ptr->value, 1000);

                /* Insert cannot fail */
                table->force_insert(rec_ptr);	
        }
        table->finish_init();
}

static void init_small_bank(hek_config config, hek_table **tables)
{
        uint32_t i, j, record_size;
        char *records;
        hek_record *rec_ptr;
        record_size = sizeof(SmallBankRecord) + sizeof(hek_record);
        for (j = 0; j < 2; ++j) {
                records = (char*)alloc_interleaved_all(record_size*config.num_records);
                memset(records, 0x0, record_size*config.num_records);
                for (i = 0; i < config.num_records; ++i) {
                        rec_ptr = (hek_record*)(records + i*record_size);
                        rec_ptr->next = NULL;
                        rec_ptr->begin = 0;
                        rec_ptr->end = HEK_INF;
                        rec_ptr->key = i;
                        rec_ptr->size = sizeof(SmallBankRecord);
                        GenRandomSmallBank(rec_ptr->value, 8);
                        tables[j]->force_insert(rec_ptr);
                }
                tables[j]->finish_init();
        }
}

/*
 * Initialize tables. Worker threads perform actual txns, they are not involved 
 * in the initialization process.
 */
static void init_tables(hek_config config, hek_table **tables)
{
        if (config.experiment < 3)
                init_ycsb(config, tables[0]);
        else
                init_small_bank(config, tables);
}

/*
 * Create tables. Allocate memory for each table. Does not initialize tables 
 * with data.
 */
static hek_table** setup_tables(hek_config config)
{
        uint64_t num_slots;
        int cpu_start, cpu_end, num_tables, i;
        hek_table **tables;
        cpu_start = 0;
        cpu_end = (int)config.num_threads-1;
        num_slots = config.num_records;
        if (config.experiment < 3) 
                num_tables = 1;
        else 
                num_tables = 2;        
        tables = (hek_table**)malloc(sizeof(hek_table*)*num_tables);
        for (i = 0; i < num_tables; ++i) 
                tables[i] = new hek_table(num_slots, cpu_start, cpu_end);
        return tables;
}


/*
 * Create hek_queues for inter-thread communication of commit dependency 
 * results.
 */
static SimpleQueue<hek_action*>*** setup_hek_queues(hek_config config)
{
        SimpleQueue<hek_action*> ***ret =
                (SimpleQueue<hek_action*>***)
                malloc(sizeof(SimpleQueue<hek_action**>)*config.num_threads);
        int i;
        for (i = 0; i < config.num_threads; ++i) 
                ret[i] = setup_queues<hek_action*>(config.num_threads, 1024);
        return ret;
}

/*
 * Compute the size of the free list for each table in the system. Encapsulated 
 * in a function so we can easily change how much to allocate later.
 */
static void compute_free_sz(hek_config config)
{
        uint64_t thread_sz;

        thread_sz = TOTAL_SIZE / config.num_threads;
        if (config.experiment < 3) {
                freelist_sizes[0] = thread_sz;
                //                freelist_sizes[0] = 1<<30;
                freelist_sizes[1] = 0;
        } else {
                freelist_sizes[0] = thread_sz/2;
                freelist_sizes[1] = thread_sz/2;
        }
}

/*
 * Workers need record sizes specified in an array. Use a global array 
 * (read-only) to communicate record sizes to workers.
 */
static void compute_record_sizes(hek_config config)
{
        if (config.experiment < 3) {
                record_sizes[0] = 1000;
                record_sizes[1] = 0;
        } else {
                record_sizes[0] = sizeof(SmallBankRecord);
                record_sizes[1] = sizeof(SmallBankRecord);
        }
}

/*
 * Return the number of tables in the given experiment. 
 */
static int num_tables(hek_config config)
{
        if (config.experiment < 3) 
                return 1;
        else if (config.experiment < 5) 
                return 2;
        else 
                return 0;        
}

/*
 * Setup workers. Allocates all the necessary data-structures for each worker 
 * thread. Threads do not actually begin running. 
 */  
static hek_worker** setup_workers(hek_config config, hek_table **tables,
                                  SimpleQueue<hek_batch> ***input_queues,
                                  SimpleQueue<hek_batch> ***output_queues)
{
        SimpleQueue<hek_batch> **inputs, **outputs;
        SimpleQueue<hek_action*> ***commit_queues, ***abort_queues;        
        hek_worker **workers;
        hek_worker_config worker_conf;
        int i;
        void *global_time;

        /* Allocate space for global counter */
        global_time = malloc(64);
        assert(global_time != NULL);
        memset(global_time, 0x0, 64);
        
        /* Common worker_conf data */
        worker_conf.global_time = (volatile uint64_t*)global_time;
        assert(*worker_conf.global_time == 0);
        worker_conf.num_tables = num_tables(config);
        worker_conf.num_threads = config.num_threads;
        worker_conf.free_list_sizes = freelist_sizes;
        worker_conf.record_sizes = record_sizes;
        worker_conf.tables = tables;
        
        /* Initialize data structures */
        workers = (hek_worker**)malloc(sizeof(hek_worker*)*config.num_threads);
        inputs = setup_queues<hek_batch>(config.num_threads, 1024);
        outputs = setup_queues<hek_batch>(config.num_threads, 1024);
        commit_queues = setup_hek_queues(config);
        abort_queues = setup_hek_queues(config);

        /* Create workers */
        for (i = 0; i < config.num_threads; ++i) {
                worker_conf.cpu = i;
                worker_conf.input_queue = inputs[i];
                worker_conf.output_queue = outputs[i];
                worker_conf.commit_queues = commit_queues[i];
                worker_conf.abort_queues = abort_queues[i];
                workers[i] = new (i) hek_worker(worker_conf);
        }

        /* Pass the caller a reference to input & output queues */
        *input_queues = inputs;
        *output_queues = outputs;
        return workers;
}

/*
 * Generate a single hek_key with "default" values. Used to reduce clutter in 
 * functions which need to create a hek_key.
 */
static hek_key create_blank_key()
{
        hek_key ret = {
                0,		/* key */
                0,		/* table_id */
                NULL,		/* txn */
                NULL,		/* value */
                NULL,		/* next */
                NULL,		/* table_ptr */
                0,		/* time */                
                0,		/* prev_ts */
                false,		/* written */
                false,		/* is_rmw */
                0,		/* txn_ts */
        };
        return ret;
}

/*
 * Generate a single read-only transaction.
 */
static hek_action* generate_readonly(hek_config config, RecordGenerator *gen)
{
        assert(config.experiment < 3 && config.read_pct > 0);
        
        uint32_t num_reads, i;
        hek_readonly_action *ret;
        hek_key to_add;
        std::set<uint64_t> seen_keys;
        UniformGenerator uniform_gen(config.num_records);
        void *mem;
        
        num_reads = config.read_txn_size;
        to_add = create_blank_key();
        if (posix_memalign(&mem, 256, sizeof(hek_readonly_action)) != 0) {
                std::cerr << "Txn initialization failed!\n";
                assert(false);
        }
        ret = new (mem) hek_readonly_action();        
        assert(((uint64_t)ret) % 256 == 0);
        for (i = 0; i < num_reads; ++i) {
                if (i < 10)
                        to_add.key = GenUniqueKey(gen, &seen_keys);
                else
                        to_add.key = GenUniqueKey(&uniform_gen, &seen_keys);
                to_add.txn = ret;
                to_add.table_id = 0;
                ret->readset.push_back(to_add);
        }
        return ret;
}


/*
 * Generate an RMW YCSB txn. 
 */
static hek_action* generate_rmw(hek_config config, RecordGenerator *gen)
{
        assert(config.experiment == 0 || config.experiment == 1);

        uint32_t i;
        hek_key to_add;
        hek_rmw_action *ret;
        std::set<uint64_t> seen_keys;
        void *mem;

        if (posix_memalign(&mem, 256, sizeof(hek_rmw_action)) != 0) {
                std::cerr << "Txn initialization failed!\n";
                assert(false);
        }
        ret = new (mem) hek_rmw_action();
        assert(((uint64_t)ret) % 256 == 0);
        to_add = create_blank_key();
        for (i = 0; i < config.txn_size; ++i) {
                to_add.key = GenUniqueKey(gen, &seen_keys);
                to_add.table_id = 0;
                to_add.txn = ret;
                to_add.is_rmw = false;
                ret->readset.push_back(to_add);
                if (config.experiment == 0 || i < RMW_COUNT) {
                        ret->writeset.push_back(to_add);
                        ret->readset[i].is_rmw = true;
                }
        }

        /* 
         * Sort keys so records are inserted in a deterministic order. 
         * Reduces the chance of an abort.  
         * std::sort(ret->writeset.begin(), ret->writeset.end(), key_cmp);
         * std::sort(ret->readset.begin(), ret->readset.end(), key_cmp);
         */
        return ret;
}

/*
 * Create a single YCSB txn. This function figures out _which_ YCSB txn to 
 * create (read-only, rmw, write-only, etc.).
 */
static hek_action* create_ycsb_single(hek_config config, RecordGenerator *gen)
{
        int flip;
        flip = (uint32_t)rand() % 100;
        assert(flip >= 0 && flip < 100);
        if (flip < config.read_pct) {
                return generate_readonly(config, gen);
        } else if (config.experiment == 0 || config.experiment == 1) {
                return generate_rmw(config, gen);
        } else if (config.experiment == 2) {                
                /* XXX this is incomplete!!! */
                assert(false);
        } else {
                std::cerr << "Invalid experiment!\n";
                assert(false);
        }
        return NULL;
}


static hek_action* create_small_bank_single(RecordGenerator *gen)
{
        using namespace hek_small_bank;
        hek_action *ret = NULL;
        int flip;
        void *mem;
        std::set<uint64_t> seen_keys;
        
        flip = rand() % 5;
        if (flip == 0) {                
                if (posix_memalign(&mem, 256, sizeof(balance)) != 0) {
                        std::cerr << "Txn initialization failed!\n";
                        assert(false);
                }
                uint64_t customer_id = GenUniqueKey(gen, &seen_keys);
                ret = new (mem) balance(customer_id, NULL);
                assert(((uint64_t)ret) % 256 == 0);
        } else if (flip == 1) {
                if (posix_memalign(&mem, 256, sizeof(deposit_checking)) != 0) {
                        std::cerr << "Txn initialization failed!\n";
                        assert(false);
                }
                uint64_t customer_id = GenUniqueKey(gen, &seen_keys);
                long amount = (long)(rand() % 25);
                ret = new (mem) deposit_checking(customer_id, amount, NULL);
        } else if (flip == 2) {
                if (posix_memalign(&mem, 256, sizeof(transact_saving)) != 0) {
                        std::cerr << "Txn initialization failed!\n";
                        assert(false);
                }
                uint64_t customer_id = GenUniqueKey(gen, &seen_keys);
                long amount = (long)(rand() % 25);
                ret = new (mem) transact_saving(customer_id, amount, NULL);
        } else if (flip == 3) {
                if (posix_memalign(&mem, 256, sizeof(amalgamate)) != 0) {
                        std::cerr << "Txn initialization failed!\n";
                        assert(false);
                }
                uint64_t from_customer = GenUniqueKey(gen, &seen_keys);
                uint64_t to_customer = GenUniqueKey(gen, &seen_keys);
                ret = new (mem) amalgamate(from_customer, to_customer, NULL);
        } else if (flip == 4) {
                if (posix_memalign(&mem, 256, sizeof(write_check)) != 0) {
                        std::cerr << "Txn initialization failed!\n";
                        assert(false);
                }
                uint64_t customer = GenUniqueKey(gen, &seen_keys);
                long amount = (long)(rand() % 25);
                if (rand() % 2 == 0) {
                        amount *= -1;
                }
                ret = new (mem) write_check(customer, amount, NULL);
        } else {
                assert(false);
        }
        return ret;
}

/*
 * Create a batch of ycsb txns. Responsible for creating a batch struct and 
 * initializing an array of ptrs to track the txns.
 */
static hek_batch create_ycsb_batch(uint32_t batch_size, hek_config config)
{
        uint32_t i;
        hek_batch batch;
        RecordGenerator *gen;

        if (config.distribution == 0) {
                gen = new UniformGenerator(config.num_records);
        } else {
                assert(config.distribution == 1);
                gen = new ZipfGenerator(config.num_records, config.theta);
        }
        batch.num_txns = batch_size;
        batch.txns = (hek_action**)alloc_mem(batch_size*sizeof(hek_action*),
                                             MAX_CPU);
        for (i = 0; i < batch_size; ++i) 
                batch.txns[i] = create_ycsb_single(config, gen);
        delete(gen);
        return batch;
}

static hek_batch create_small_bank_batch(uint32_t batch_size, hek_config config)
{
        uint32_t i;
        hek_batch batch;
        RecordGenerator *gen;

        if (config.distribution == 0) {
                gen = new UniformGenerator(config.num_records);
        } else {
                assert(config.distribution == 1);
                gen = new ZipfGenerator(config.num_records, config.theta);
        }
        batch.num_txns = batch_size;
        batch.txns = (hek_action**)alloc_mem(batch_size*sizeof(hek_action*),
                                             MAX_CPU);
        for (i = 0; i < batch_size; ++i)
                batch.txns[i] = create_small_bank_single(gen);
        delete(gen);
        return batch;
}

/*
 * Create a batch of txns. Responsible for creating either YCSB or SmallBank 
 * txns.
 */
static hek_batch create_single_batch(uint32_t batch_size, hek_config config)
{
        assert(config.experiment <= 4);
        if (config.experiment < 3)
                return create_ycsb_batch(batch_size, config);
        else
                return create_small_bank_batch(batch_size, config);
}

/*
 * Given "total_txns" for the system to run, divide them among the set of worker
 * threads and return a batch of txns for each worker. 
 */
static hek_batch* create_single_round(hek_config config, uint32_t total_txns)
{
        uint32_t batch_size, remainder, i;
        hek_batch *ret;
        
        batch_size = total_txns / config.num_threads;
        remainder = total_txns % config.num_threads;
        ret = (hek_batch*)malloc(sizeof(hek_batch)*config.num_threads);
        for (i = 0; i < config.num_threads; ++i) {
                if (i == config.num_threads - 1)
                        batch_size += remainder;
                ret[i] = create_single_batch(batch_size, config);
        }
        return ret;
}

/*
 * Creates two rounds of batches. One for warm up. One for the actual 
 * experiment.
 */
static vector<hek_batch*> setup_txns(hek_config config)
{
        uint32_t warmup_batch_sz;
        vector<hek_batch*> ret;
        warmup_batch_sz = 1000;
        ret.push_back(create_single_round(config, warmup_batch_sz));
        ret.push_back(create_single_round(config, config.num_txns));
        ret.push_back(create_single_round(config, config.num_txns));
        return ret;
}

/* Run workers. */
static void init_workers(hek_worker **workers, uint32_t num_workers)
{
        uint32_t i;
        for (i = 0; i < num_workers; ++i) {
                workers[i]->Run();
                workers[i]->WaitInit();
        }
}

/* Wait for each worker to finish a single batch. */
static uint32_t end_single_round(SimpleQueue<hek_batch> **outputs,
                                 uint32_t num_outputs)
{
        uint32_t i, num_txns;
        hek_batch out_batch;
        
        num_txns = 0;
        for (i = 0; i < num_outputs; ++i) {
                out_batch = outputs[i]->DequeueBlocking();
                num_txns += out_batch.num_txns;
        }
        return num_txns;
}

/* Enqueue a single batch into each worker's input queue.  */
static void start_single_round(SimpleQueue<hek_batch> **input_queues,
                               hek_batch *inputs,
                               uint32_t num_inputs)
{
        uint32_t i;
        for (i = 0; i < num_inputs; ++i)
                (input_queues[i])->EnqueueBlocking(inputs[i]);
}

/* Run experiment, measure time elapsed.  */
static struct hek_result run_experiment(hek_config config,
                                        vector<hek_batch*> input,
                                        hek_worker **workers,
                                        SimpleQueue<hek_batch> **input_queues,
                                        SimpleQueue<hek_batch> **output_queues)
{
        struct timespec start_time, end_time;
        struct hek_result result;
        uint32_t num_txns;
        
        init_workers(workers, config.num_threads);

        /* Warm up run. */
        start_single_round(input_queues, input[0], config.num_threads);
        end_single_round(output_queues, config.num_threads);

        /* Real run. */
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_time);
        barrier();
        start_single_round(input_queues, input[1], config.num_threads);
        start_single_round(input_queues, input[2], config.num_threads);
        num_txns = end_single_round(output_queues, config.num_threads);
        barrier();
        clock_gettime(CLOCK_THREAD_CPUTIME_ID, &end_time);
        barrier();

        /* Write to result struct.  */
        result.elapsed_time = diff_time(end_time, start_time);
        result.num_txns = num_txns;
        return result;
}

/* Write results to an output file. */
static void write_results(struct hek_result result, hek_config config)
{
        double elapsed_milli;
        timespec elapsed_time;
        std::ofstream result_file;
        elapsed_time = result.elapsed_time;
        elapsed_milli =
                1000.0*elapsed_time.tv_sec + elapsed_time.tv_nsec/1000000.0;
        std::cout << elapsed_milli << '\n';
        result_file.open("hek.txt", std::ios::app | std::ios::out);
        result_file << "time:" << elapsed_milli << " txns:" << result.num_txns;
        result_file << " threads:" << config.num_threads << " hek ";
        result_file << "records:" << config.num_records << " ";
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


/* "main" function for hekaton */
void do_hekaton_experiment(hek_config config)
{
        hek_table **tables;
        hek_worker **workers;
        vector<hek_batch*> inputs;
        SimpleQueue<hek_batch> **input_queues, **output_queues;
        struct hek_result result;

        compute_free_sz(config);
        compute_record_sizes(config);
        tables = setup_tables(config);
        std::cerr << "Done setting up tables!\n";
        init_tables(config, tables);
        std::cerr << "Done initializing tables!\n";
        workers = setup_workers(config, tables, &input_queues, &output_queues);
        std::cerr << "Done setting up workers!\n";
        inputs = setup_txns(config);
        std::cerr << "Done setting up transactions!\n";
        pin_memory();        
        result = run_experiment(config, inputs, workers, input_queues,
                                output_queues);
        write_results(result, config);
}

