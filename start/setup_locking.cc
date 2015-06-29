#include <setup_workload.h>
#include <common.h>
#include <db.h>
#include <locking_action.h>
#include <lock_manager.h>
#include <eager_worker.h>

typedef SimpleQueue<locking_action_batch> locking_queue;

struct locking_result {
        double elapsed_time;
        uint64_t num_txns;
};

static locking_action* txn_to_action(txn *t)
{
        locking_action *ret;
        struct big_key *arr;
        uint32_t i, num_reads, num_writes, num_rmws, max;

        arr = setup_array(t);
        ret = new locking_action(t);

        num_reads = t->num_reads();
        t->get_reads(arr);
        for (i = 0; i < num_reads; ++i) 
                ret->add_read_key(arr[i]->key, arr[i]->table_id);

        num_rmws = t->num_rmws();
        t->get_rmws(arr);
        for (i = 0; i < num_rmws; ++i) 
                ret->add_write_key(arr[i]->key, arr[i]->table_id);

        num_writes = t->num_writes();
        t->get_writes(arr);
        for (i = 0; i < num_writes; ++i) 
                ret->add_write_key(arr[i]->key, arr[i]->table_id);
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
                ret.batch[i] = generate_transaction(w_conf);        
        return ret;
}

static locking_action_batch** setup_input(locking_config conf,
                                          workload_config w_conf)
{
        
}

static locking_worker** setup_workers(locking_queue **input,
                                      locking_queue **output,
                                      LockManager *mgr,
                                      int num_threads,
                                      int num_pending,
                                      Table **tables)
{
        assert(mgr != NULL && tables != NULL);
        
        locking_worker **ret;
        int i;

        ret = (locking_worker**)malloc(sizeof(locking_worker*)*num_threads);
        assert(ret != NULL);
        for (i = 0; i < num_threads; ++i) {
                struct locking_worker_conf conf = {
                        mgr,
                        input[i],
                        output[i],
                        i,
                        num_pending,
                        tables,                        
                };
                ret[i] = new(i) locking_worker(conf);
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

void write_locking_output(locking_config conf, struct locking_result result)
{
        
}

static struct locking_result do_measurement(locking_config conf,
                                            locking_worker **workers,
                                            locking_queue **input,
                                            locking_queue **output)
{
        
}

