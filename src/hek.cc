#include <hek.h>

hek_queue::hek_queue()
{
        this->head = NULL;
        this->tail = &head;
}

void hek_queue::enqueue(hek_action *txn)
{
        hek_action *prev;
        barrier();
        txn->next = NULL;
        barrier();
        prev = xchgq(&this->tail, &txn->next);
        if (prev != NULL) 
                *prev = txn;
}

hek_action* hek_queue::dequeue_batch()
{
        hek_action *ret, *iter, **old_tail;
        volatile hek_action *iter;
        ret = head;        
        head = NULL;
        old_tail = xchgq(this->tail, &head);
        iter = &head;
        while (old_tail != iter)
                while (true) {
                        barrier();
                        if (*iter != NULL)
                                break;
                        barrier();
                }
                iter = &(*iter)->next;
        return ret;
}

void hek_worker::check_dependents()
{
        hek_action *aborted, *committed;
        aborted = config.abort_queue->dequeue_batch();
        barrier();
        while (aborted != NULL) {
                assert(aborted->flag == ABORT);
                aborted->state = ABORT;
                do_abort(aborted);
                aborted = aborted->next;
        }
        committed = config.commit_queue->dequeue_batch();
        barrier();
        while (committed != NULL) {
                assert(committed->flag == COMMIT && committed->count == 0);
                committed->state = COMMIT;
                do_commit(committed);
                committed = committed->next;
        }
}

void hek_worker::StartWorking()
{
        uint32_t i, batch_size;
        struct hek_batch batch;
        while (true) {
                num_committed = 0;
                num_done = 0;
                batch = input_queue->DequeueBlocking();
                for (i = 0; i < batch.num_txns; ++i) {
                        run_txn(batch.txns[i]);
                        check_dependents();                
                }
                while (num_done != batch->num_txns) 
                        check_dependents();
                batch.num_txns = num_committed;
                output_queue->EnqueueBlocking(batch);
        }
}

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

void hek_worker::get_reads(hek_action *txn)
{
        uint32_t num_reads, i, table_id;
        uint64_t key, ts;        
        struct hek_record *read_record;
        ts = txn->begin;
        num_reads = txn->readset.size();
        for (i = 0; i < num_reads; ++i) {
                table_id = txn->readset[i].table_id;
                key = txn->readset[i].key;
                read_record = config.tables[table_id]->GetVersion(key, ts);
                txn->readset[i].value = read_record;
                txn->readset[i].time = *BEGIN_TS_FIELD(read_record);
        }
}

bool hek_worker::add_commit_dep(hek_action *dependency, hek_action *dependent,
                                hek_key *key)
{
        bool success, ret;
        success = false;
        ret = true;
        hek_latch(dependency);	 // latch to atomically add dependency 
        if (HEK_TIME(dependency->end) == key->time &&
            HEK_STATE(dependency->end) == PREPARING) {
                success = true;
                *dependency->wakeups = key;
                dependency->wakeups = &key->next;
        }
        ret = (HEK_STATE(dependency->end) == PREPARING ||
               HEK_STATE(dependency->end) == COMMIT) &&
                HEK_TIME(dependency->end) == key->time;
        hek_unlatch(dependency);
        if (success)
                fetch_and_increment(&dependent->dep_count);
        return ret;
}

bool hek_worker::validate_single(hek_action *txn, hek_key *key,
                                 hek_record *read_record)
{
        volatile uint64_t read_begin;
        barrier();
        read_begin = *BEGIN_TS_FIELD(read_record);
        barrier();
        if (IS_TIMESTAMP(read_begin)) {
                if (GET_HEK_TIMESTAMP(read_begin) != txn->readset[i].time)
                        return false;
        } else {
                writer = (hek_action*)GET_HEK_TXN(read_begin);
                if (HEK_TIME(writer->end) != txn->readset[i].time ||
                    add_commit_dep(writer, txn, &txn->readset[i]) == false)
                        return false;
        }
}

bool hek_worker::validate_reads(hek_action *txn)
{
        uint32_t num_reads, i, table_id;
        uint64_t key, ts;
        volatile uint64_t read_begin;
        struct hek_record *read_record;
        
        ts = txn->end;
        num_reads = txn->readset.size();
        for (i = 0; i < num_reads; ++i) {
                table_id = txn->readset[i].table_id;
                key = txn->readset[i].key;
                read_record = config.tables[table_id]->GetVersion(key, ts);
                if (read_record != txn->readset[i].value)
                        return false;
        }
        return true;
}

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

void hek_worker::run_txn(hek_action *txn)
{
        hek_status status;
        bool validated;
        
        transition_begin(txn);
        txn->begin = fetch_and_increment(config.global_time);
        get_reads(txn);        
        status = txn->run();
        if (status.validation == false)
                goto abort;
        transition_preparing(txn);
        validated = validate_reads(txn);
        if (validated == true) {
                if (txn->dependent == false) {
                        transition_commit(txn);
                        do_commit(txn);
                }
        } 
 abort:
        transition_abort(txn);
        do_abort(txn);
        return false;
}

void hek_worker::kill_waiters(hek_action *txn)
{
        hek_record *wait_record;
        hek_action *waiter;
        while (wait_record != NULL) {
                waiter = wait_record->txn;                
                if (waiter->dep_flag == PREPARING &&
                    cmp_and_swap(&waiter->dep_flag, PREPARING, ABORT)) {
                        assert(waiter->dep_count > 0);
                        fetch_and_decrement(&waiter->dep_count);
                        insert_kill_queue(waiter);
                }
                wait_record = wait_record->next;
        }
}

void hek_worker::commit_waiters(hek_action *txn)
{
        hek_record *wait_record;
        hek_action *waiter;
        volatile uint64_t flag;
        while (wait_record != NULL) {
                waiter = wait_record->txn;
                if (fetch_and_decrement(&waiter->dep_count) == 0) {
                        barrier();
                        flag = waiter->dep_flag;
                        barrier();
                        if (flag == PREPARING)
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
        uint32_t num_writes, i, table_id;
        hek_key *key;
        hek_record *record;
        uint64_t prev_ts;
        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                key = &txn->writeset[i];
                assert(key->is_written == true);
                assert(key->record != NULL);
                assert(key->table_id < config.num_tables);
                config.tables[table_id]->finalize_version(record, txn->end);
        }
        
}

void hek_worker::remove_writes(hek_action *txn)
{
        uint32_t num_writes, i, table_id;
        hek_key *key;
        hek_record *record;
        uint64_t prev_ts;
        num_writes = txn->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                key = &txn->writeset[i];
                if (key->is_written == true) {
                        assert(record != NULL);
                        assert(table_id < config.num_tables);
                        table_id = key->table_id;
                        prev_ts = key->prev_ts;
                        record = key->record;
                        config.tables[table_id]->remove_version(record);
                }
        }
}
