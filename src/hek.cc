#include <hek.h>
#include <hek_table.h>
#include <hek_action.h>
#include <hek_record.h>

static void init_list(char *start, uint32_t num_records, uint32_t record_sz)
{
        uint32_t i;
        hek_record *cur, *next;
        
        assert(num_records > 0);
        cur = NULL;
        next = NULL;
        for (i = 0; i < num_records; ++i) {
                cur = (hek_record*)start;
                start += record_sz + sizeof(hek_record);
                next = (hek_record*)start;
                cur->next = next;
        }
        cur->next = NULL;
}

/*
 * Initialize the record allocator. Works in two phases, first do the 
 * allocation, then link up everything.
 */
void hek_worker::init_allocator()
{
        uint32_t record_sz, header_sz, i;
        char *temp, *start;
        uint64_t total_sz, free_list_sz, num_elems;

        header_sz = sizeof(hek_record);
        total_sz = config.num_tables*sizeof(hek_record*);        
        for (i = 0; i < this->config.num_tables; ++i) {
                free_list_sz = config.free_list_sizes[i];
                record_sz = config.record_sizes[i];
                num_elems = free_list_sz / (record_sz + header_sz);
                total_sz += num_elems * (record_sz + header_sz);
        }
        temp = (char*)alloc_interleaved_all(total_sz);
        records = (hek_record**)temp;
        start = temp + config.num_tables*sizeof(hek_record*);
        for (i = 0; i < this->config.num_tables; ++i) {
                records[i] = (hek_record*)start;
                free_list_sz = config.free_list_sizes[i];
                record_sz = config.record_sizes[i];
                num_elems = free_list_sz / (record_sz + header_sz);
                init_list(start, num_elems, record_sz);
                start += num_elems * (record_sz + header_sz);
        }
}

hek_worker::hek_worker(hek_worker_config config) : Runnable(config.cpu)
{
        this->config = config;
        init_allocator();
}



void hek_worker::insert_commit_queue(hek_action *txn)
{
        hek_queue *queue = txn->worker->config.commit_queue;
        queue->enqueue(txn);
}

void hek_worker::insert_abort_queue(hek_action *txn)
{
        hek_queue *queue = txn->worker->config.abort_queue;
        queue->enqueue(txn);
}

void hek_worker::Init()
{
}

// A hek_queue is used to communicate the result of a commit dependency to a
// dependent transaction.
hek_queue::hek_queue()
{
        this->head = NULL;
        this->tail = &head;
}

// Insert a single transaction into the queue. First change the tail, then add a
// link pointer. Non-blocking.
void hek_queue::enqueue(hek_action *txn)
{
        hek_action **prev;
        barrier();
        txn->next = NULL;
        barrier();
        prev = (hek_action**)xchgq((volatile uint64_t*)&this->tail,
                                   (uint64_t)&txn->next);
        if (prev != NULL) 
                *prev = txn;
}

// Dequeue several transactions. 
hek_action* hek_queue::dequeue_batch()
{
        hek_action *ret, **old_tail;
        volatile hek_action **iter;
        ret = (hek_action*)head;
        barrier();
        head = NULL;
        barrier();
        old_tail = (hek_action**)xchgq((volatile uint64_t*)&this->tail,
                                       (uint64_t)&head);
        iter = &head;
        while (old_tail != iter) {
                while (true) {
                        barrier();
                        if (*iter != NULL)
                                break;
                        barrier();
                }
                iter = (volatile hek_action**)&(*iter)->next;
        }
        return ret;
}

// Check the result of dependent transactions.
void hek_worker::check_dependents()
{
        hek_action *aborted, *committed;
        aborted = config.abort_queue->dequeue_batch();
        barrier();
        while (aborted != NULL) {
                assert(aborted->dep_flag == ABORT);
                transition_abort(aborted);
                do_abort(aborted);
                aborted = (hek_action*)aborted->next;
        }
        committed = config.commit_queue->dequeue_batch();
        barrier();
        while (committed != NULL) {
                assert(committed->dep_flag == COMMIT &&
                       committed->dep_count == 0);
                transition_commit(committed);
                do_commit(committed);
                committed = (hek_action*)committed->next;
        }
}

// Hekaton worker threads's "main" function.
void hek_worker::StartWorking()
{
        uint32_t i;
        struct hek_batch input_batch, output_batch;
        
        output_batch.txns = NULL;
        while (true) {
                num_committed = 0;
                num_done = 0;
                input_batch = config.input_queue->DequeueBlocking();
                for (i = 0; i < input_batch.num_txns; ++i) {
                        run_txn(input_batch.txns[i]);
                        check_dependents();                
                }

                /* Wait for all txns with commit dependencies. */
                while (num_done != input_batch.num_txns) 
                        check_dependents();
                output_batch.num_txns = num_committed;
                config.output_queue->EnqueueBlocking(output_batch);
        }
}

//
// A transition can proceed only after acquiring the transaction's latch.
// A transaction's state changes from EXECUTING->PREPARING->COMMITTED/ABORTED.
// Commit dependencies can only be added in state PREPARING.
//
void hek_worker::transition_begin(hek_action *txn)
{
        lock(&txn->latch);
        txn->end = EXECUTING;
        unlock(&txn->latch);
}

void hek_worker::transition_preparing(hek_action *txn)
{
        uint64_t end_ts;
        end_ts = fetch_and_increment(config.global_time);
        end_ts = CREATE_PREP_TIMESTAMP(end_ts);
        lock(&txn->latch);
        txn->end = end_ts;
        unlock(&txn->latch);
}

void hek_worker::transition_commit(hek_action *txn)
{
        uint64_t time;
        assert(HEK_STATE(txn->end) == PREPARING);
        time = HEK_TIME(txn->end);
        time = CREATE_COMMIT_TIMESTAMP(time);
        lock(&txn->latch);        
        txn->end = time;
        unlock(&txn->latch);
}

void hek_worker::transition_abort(hek_action *txn)
{
        uint64_t time;
        
        assert(HEK_STATE(txn->end) == PREPARING);
        time = HEK_TIME(txn->end);
        time = CREATE_ABORT_TIMESTAMP(time);
        lock(&txn->latch);
        txn->end = time;
        unlock(&txn->latch);
}

/* Give a transaction a new record for every write it performs. */
void hek_worker::get_writes(hek_action *txn)
{
        uint32_t num_writes, i, table_id;
        struct hek_record *write_record;

        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                table_id = txn->writeset[i].table_id;
                write_record = get_new_record(table_id);
                write_record->key = txn->writeset[i].key;
                txn->writeset[i].value = write_record;
        }
}

//
// Runs before txn logic begins. Keep a reference to every record read for
// validation.
//
void hek_worker::get_reads(hek_action *txn)
{
        uint32_t num_reads, i, table_id;
        uint64_t key, ts, *begin_ptr;
        struct hek_record *read_record;
        
        ts = txn->begin;
        num_reads = txn->readset.size();
        for (i = 0; i < num_reads; ++i) {
                table_id = txn->readset[i].table_id;
                key = txn->readset[i].key;
                begin_ptr = &txn->readset[i].time;
                read_record = config.tables[table_id]->get_version(key, ts,
                                                                   begin_ptr);
                txn->readset[i].value = read_record;
        }
}

/*
 * Each transaction's state is protected by a lock. We need locks because of
 * commit dependencies; a commit dependency can only be added if the transaction
 * is in state PREPARING. We can't atomically ensure that the transaction's
 * state is PREPARING and enqueue the commit dependency without locks.
 */
void hek_worker::add_commit_dep(hek_action *out, hek_key *key, hek_action *in)
{
        assert(!IS_TIMESTAMP(key->time) && in == GET_TXN(key->time));
        assert(HEK_STATE(out->end) == PREPARING);
        assert(HEK_STATE(in->end) >= PREPARING);

        lock(&in->latch);
        if (HEK_STATE(in->end) == PREPARING) {
                key->next = in->dependents;
                in->dependents = key;
        } else if (HEK_STATE(in->end) == ABORT &&
                   cmp_and_swap(&out->dep_flag, PREPARING, ABORT)) {
                config.abort_queue->enqueue(out);
        } 
        unlock(&in->latch);
}

bool hek_worker::validate_single(hek_action *txn, hek_key *key)
{
        assert(!IS_TIMESTAMP(txn->end) && HEK_STATE(txn->end) == PREPARING);
        struct hek_record *vis_record, *read_record;
        hek_action *preparing;
        uint64_t vis_ts, read_ts, record_key, end_ts;
        uint32_t table_id;

        end_ts = HEK_TIME(txn->end);
        table_id = key->table_id;
        record_key = key->key;
        read_record = key->value;
        read_ts = key->time;
        vis_record = config.tables[table_id]->get_version(record_key, end_ts,
                                                          &vis_ts);
        if (vis_record == read_record) {
                if (IS_TIMESTAMP(read_ts)) {
                        return read_ts == vis_ts;
                } else if (!IS_TIMESTAMP(vis_ts) && vis_ts == read_ts) {
                        key->txn = txn;
                        add_commit_dep(txn, key, GET_TXN(vis_ts));
                        return true;
                } else if (IS_TIMESTAMP(vis_ts)) {
                        preparing = GET_TXN(read_ts);
                        return HEK_TIME(preparing->begin) == vis_ts;
                }
        }
        return false;
}

hek_record* hek_worker::get_new_record(uint32_t table_id)
{
        hek_record *ret;

        ret = records[table_id];
        assert(ret != NULL);
        records[table_id] = ret->next;
        ret->next = NULL;
        return ret;       
}

void hek_worker::return_record(uint32_t table_id, hek_record *record)
{
        record->next = records[table_id];
        records[table_id] = record;
}

bool hek_worker::validate_reads(hek_action *txn)
{
        assert(!IS_TIMESTAMP(txn->end));
        assert(HEK_STATE(txn->end) == PREPARING);
        uint32_t num_reads, i;
        
        txn->must_wait = false;
        fetch_and_increment(&txn->dep_count);
        num_reads = txn->readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (!validate_single(txn, &txn->readset[i]))
                        return false;
        }
        fetch_and_decrement(&txn->dep_count);
        return true;
}

/*
void hek_worker::install_writes(hek_action *txn)
{
        uint32_t num_writes, i, table_id;
        uint64_t key;
        void *prev_ptr, *cur_ptr;
        for (i = 0; i < num_writes; ++i) {
                xchgq(END_TS_FIELD(prev_ptr), txn->end);
                xchgq(BEGIN_TS_FIELD(cur_ptr), txn->end);
        }
}
*/

/* 
 * Insert records written by a transaction. If all insertions succeed, it does 
 * not mean that the txn will commit. Reads must still be validated, and writes 
 * subsequently finalized.      
 */
bool hek_worker::insert_writes(hek_action *txn)
{
        uint32_t num_writes, i, tbl_id;
        hek_table *table;
        hek_record *rec;

        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(txn->writeset[i].written == false);
                rec = txn->writeset[i].value;
                rec->begin = (uint64_t)txn | 0x1;
                rec->end = HEK_INF;
                tbl_id = txn->writeset[i].table_id;
                table = config.tables[tbl_id];
                if (!table->insert_version(rec)) 
                        return false;                
                else
                        txn->writeset[i].written = true;
        }
        return true;
}

// 1. Run txn logic (may abort due to write-write conflicts)
// 2. Validate reads
// 3. Check if the txn depends on others. If yes, wait for commit dependencies,
// otherwise, abort.
// 
void hek_worker::run_txn(hek_action *txn)
{
        hek_status status;
        bool validated;
        
        transition_begin(txn);
        txn->begin = fetch_and_increment(config.global_time);
        get_reads(txn);
        get_writes(txn);
        status = txn->Run();
        transition_preparing(txn);
        if (!insert_writes(txn))
                goto abort;
        validated = validate_reads(txn);
        if (validated == true) {
                if (txn->must_wait == false) {
                        transition_commit(txn);
                        do_commit(txn);
                }
                return;
        } 
 abort:
        transition_abort(txn);
        do_abort(txn);
}

void hek_worker::kill_waiters(hek_action *txn)
{
        hek_key *wait_record;
        hek_action *waiter;
        uint64_t state;

        wait_record = txn->dependents;
        while (wait_record != NULL) {
                waiter = wait_record->txn;
                barrier();
                state = waiter->dep_flag;
                barrier();
                if (state == PREPARING &&
                    cmp_and_swap(&waiter->dep_flag, PREPARING, ABORT)) {
                        assert(waiter->dep_count > 0);
                        fetch_and_decrement(&waiter->dep_count);
                        insert_abort_queue(waiter);
                }
                wait_record = wait_record->next;
        }
}

/*
 *  Decrease dependency count of each txn in the dependents list. 
 */
void hek_worker::commit_waiters(hek_action *txn)
{
        hek_key *wait_record;
        hek_action *waiter;
        volatile uint64_t flag;

        wait_record = txn->dependents;
        while (wait_record != NULL) {
                waiter = wait_record->txn;
                if (fetch_and_decrement(&waiter->dep_count) == 0) {
                        barrier();
                        flag = waiter->dep_flag;
                        barrier();
                        assert(flag == PREPARING);
                        insert_commit_queue(waiter);
                }
                wait_record = wait_record->next;
        }
}

void hek_worker::do_abort(hek_action *txn)
{
        assert(HEK_STATE(txn->end) == ABORT);
        remove_writes(txn);
        kill_waiters(txn);
        num_done += 1;
}

void hek_worker::do_commit(hek_action *txn)
{
        assert(HEK_STATE(txn->end) == COMMIT);
        install_writes(txn);
        commit_waiters(txn);
        num_committed += 1;
        num_done += 1;
}

void hek_worker::install_writes(hek_action *txn)
{
        uint32_t num_writes, i;
        hek_key *key;
        //        uint64_t prev_ts;
        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                key = &txn->writeset[i];
                assert(key->written == true);
                assert(key->value != NULL);
                assert(key->table_id < config.num_tables);
                assert(GET_TXN(key->value->begin) == txn);
                config.tables[key->table_id]->
                        finalize_version(key->value, HEK_TIME(txn->end));
        }
        
}

/*
 * If a transaction aborts, remove any versions it may have inserted into the 
 * table.
 */
void hek_worker::remove_writes(hek_action *txn)
{
        uint32_t num_writes, i, table_id;
        hek_key *key;
        hek_record *record;
        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                if (txn->writeset[i].written == true) {
                        key = &txn->writeset[i];
                        record = key->value;
                        table_id = key->table_id;
                        assert(record != NULL);
                        assert(table_id < config.num_tables);
                        record = key->value;
                        config.tables[table_id]->remove_version(record);
                                                       
                } else {
                        break;
                }                
        }
}
