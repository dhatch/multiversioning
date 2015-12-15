#include <occ_action.h>
#include <algorithm>
#include <occ.h>

static bool try_acquire_single(volatile uint64_t *lock_ptr)
{
        volatile uint64_t cmp_tid, locked_tid;
        barrier();
        cmp_tid = *lock_ptr;
        barrier();
        if (IS_LOCKED(cmp_tid))
                return false;        
        locked_tid = (cmp_tid | 1);
        return cmp_and_swap(lock_ptr, cmp_tid, locked_tid);
}

static void acquire_single(volatile uint64_t *lock_ptr)
{
        uint32_t backoff, temp;
        if (USE_BACKOFF) 
                backoff = 1;
        while (true) {
                if (try_acquire_single(lock_ptr)) {
                        assert(IS_LOCKED(*lock_ptr));
                        break;
                }
                if (USE_BACKOFF) {
                        temp = backoff;
                        while (temp-- > 0)
                                single_work();
                        backoff = backoff*2;
                }
        }
}

static void release_single(volatile uint64_t *lock_word)
{
        uint64_t old_tid, xchged_tid;
        
        old_tid = *lock_word;
        assert(IS_LOCKED(old_tid));
        old_tid = GET_TIMESTAMP(old_tid);
        xchged_tid = xchgq(lock_word, old_tid);
        assert(IS_LOCKED(xchged_tid));
        assert(GET_TIMESTAMP(xchged_tid) == old_tid);
}

occ_composite_key::occ_composite_key(uint32_t table_id, uint64_t key,
                                     bool is_rmw)
{
        this->tableId = table_id;
        this->key = key;
        this->is_rmw = is_rmw;
        this->is_locked = false;
        this->is_initialized = false;
}

void* occ_composite_key::GetValue() const
{
        uint64_t *temp = (uint64_t*)value;
        return &temp[1];
}

void* occ_composite_key::StartRead()
{

        volatile uint64_t *tid_ptr;
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                this->old_tid = *tid_ptr;
                barrier();
                if (!IS_LOCKED(this->old_tid))
                        break;
        }
        return (void*)&tid_ptr[1];

        //        return (void*)&((uint64_t*)value)[1];
}

bool occ_composite_key::FinishRead()
{
        return true;
        /*
        assert(!IS_LOCKED(this->old_tid));
        volatile uint64_t tid;
        bool is_valid = false;
        barrier();        
        tid = *(volatile uint64_t*)value;
        barrier();
        is_valid = (tid == this->old_tid);
        assert(!is_valid || !IS_LOCKED(tid));
        return is_valid;
                //        return true;
                */
}

uint64_t occ_composite_key::GetTimestamp()
{
        return old_tid;        
}

bool occ_composite_key::ValidateRead()
{
        assert(!IS_LOCKED(old_tid));
        volatile uint64_t *version_ptr;
        volatile uint64_t cur_tid;
        version_ptr = (volatile uint64_t*)value;
        barrier();
        cur_tid = *version_ptr;
        barrier();
        if ((GET_TIMESTAMP(cur_tid) != old_tid) ||
            (IS_LOCKED(cur_tid) && !is_rmw))
                return false;
        return true;
}

void OCCAction::add_read_key(uint32_t tableId, uint64_t key) 
{        
        occ_composite_key k(tableId, key, false);
        readset.push_back(k);
}

OCCAction::OCCAction(txn *txn) : translator(txn)
{
}

void OCCAction::add_write_key(uint32_t tableId, uint64_t key, bool is_rmw)
{
        occ_composite_key k(tableId, key, is_rmw);
        writeset.push_back(k);
        shadow_writeset.push_back(k);
}

void OCCAction::set_allocator(RecordBuffers *bufs)
{
        this->record_alloc = bufs;
}

void OCCAction::set_tables(Table **tables)
{
        this->tables = tables;
}

/*
readonly_action::readonly_action()
{
        memset(__reads, 0x0, 1000);
}

occ_txn_status readonly_action::Run()
{
        uint32_t num_reads, i, j;
        occ_txn_status status;
        char *read_ptr;
        num_reads = readset.size();
        status.commit = true;
        status.validation_pass = true;
        for (i = 0; i < num_reads; ++i) {
                read_ptr = (char*)readset[i].StartRead();
                for (j = 0; j < 10; ++j) {
                        uint64_t *write_p = (uint64_t*)&__reads[j*100];
                        *write_p += *((uint64_t*)&read_ptr[j*100]);
                }
                if (readset[i].FinishRead() == false) {
                        status.validation_pass = false;
                        break;
                }                
        }
        return status;        
}

mix_occ_action::mix_occ_action() : readonly_action()
{
}


occ_txn_status mix_occ_action::Run()
{
        //        uint32_t i, j, num_reads, num_writes;
        occ_txn_status status;
        status.commit =true;
        status.validation_pass = true;

        num_reads = readset.size();
        num_writes = writeset.size();
        for (i = 0; i < num_reads; ++i) {
                for (j = 0; j < 10; ++j) {
                        uint32_t *write_p = (uint32_t*)&__reads[j*100];
                        *write_p += *((uint32_t*)&__reads[j*100]);
                }
        }
        for (i = 0; i < num_writes; ++i) {
                char *write_ptr = (char*)writeset[i].GetValue();
                memcpy(write_ptr, __reads, 1000);
                for (j = 0; j < 10; ++j) {
                        *((uint32_t*)&write_ptr[j*100]) += (i+j);
                }
        }

        return status;
}



bool RMWOCCAction::DoReads()
{
        uint32_t num_writes, num_reads, i, j, num_fields;
        char *read_ptr, *write_ptr;
        uint64_t counter;
        
        num_reads = readset.size();
        num_writes = writeset.size();
        counter = 0;
        num_fields = 10;
        for (i = 0; i < num_reads; ++i) {
                read_ptr = (char*)readset[i].StartRead();
                barrier();
                for (j = 0; j < num_fields; ++j) 
                        counter += *((uint64_t*)&read_ptr[j*100]);
                barrier();
                if (readset[i].FinishRead() == false) 
                        return false;
        }
        for (i = 0; i < num_writes; ++i) {
                write_ptr = (char*)writeset[i].GetValue();
                read_ptr = (char*)readset[i].StartRead();
                barrier();
                memcpy(write_ptr, read_ptr, 1000);
                if (readset[i].FinishRead() == false)
                        return false;
                barrier();
                for (j = 0; j < num_fields; ++j) {
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
                }
        }
        return true;                
}

void RMWOCCAction::AccumulateValues()
{

        uint32_t i, num_fields;
        num_fields = recordSize/sizeof(uint64_t);
        __total = 0;
        for (i = 0; i < num_fields; ++i) 
                __total += __accumulated[i];

}

void RMWOCCAction::DoWrites()
{
        uint64_t counter;
        uint32_t i, j, num_writes, num_fields;
        uint64_t *field_ptr;
        num_fields = recordSize/sizeof(uint64_t);
        counter = __total;
        num_writes = writeset.size();
        for (i = 0; i < num_writes; ++i) {
                field_ptr = (uint64_t*)writeset[i].GetValue();
                for (j = 0; j < num_fields; ++j) 
                        field_ptr[j] = counter+j;
        }
}

void* RMWOCCAction::GetData()
{
        return __accumulated;
}

occ_txn_status RMWOCCAction::Run()
{
        assert(recordSize == 1000);
        occ_txn_status status;
        status.validation_pass = false;
        status.commit = true;
        status.validation_pass = DoReads();
        return status;
}
*/


uint64_t OCCAction::stable_copy(uint64_t key, uint32_t table_id, void *record)
{
        volatile uint64_t *tid_ptr;
        uint32_t record_size;
        uint64_t ret, after_read;
        void *value;

        value = this->tables[table_id]->Get(key);
        record_size = REAL_RECORD_SIZE(this->tables[table_id]->RecordSize());
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                ret = *tid_ptr;
                barrier();
                if (!IS_LOCKED(ret)) {
                        memcpy(RECORD_VALUE_PTR(record),
                               RECORD_VALUE_PTR(value), record_size);
                        barrier();
                        after_read = *tid_ptr;
                        barrier();
                        if (after_read == ret)
                                return ret;
                        else
                                throw occ_validation_exception(READ_ERR);
                        return ret;
                }
        }
}

void OCCAction::validate_single(occ_composite_key &comp_key)
{
        assert(!IS_LOCKED(comp_key.old_tid));
        void *value;
        volatile uint64_t *version_ptr;
        uint64_t cur_tid;
        
        value = tables[comp_key.tableId]->Get(comp_key.key);
        version_ptr = (volatile uint64_t*)value;
        barrier();
        cur_tid = *version_ptr;
        barrier();

        if ((GET_TIMESTAMP(cur_tid) != comp_key.old_tid) ||
            (IS_LOCKED(cur_tid) && !comp_key.is_rmw))
                throw occ_validation_exception(VALIDATION_ERR);
}

void OCCAction::validate()
{
        uint32_t num_reads, num_writes, i;

        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) 
                validate_single(this->readset[i]);
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i)
                if (this->writeset[i].is_rmw)
                        validate_single(this->writeset[i]);
}

void* OCCAction::write_ref(uint64_t key, uint32_t table_id)
{
        uint64_t tid;
        void *record;
        uint32_t i, num_writes;
        occ_composite_key *comp_key;        

        num_writes = this->writeset.size();
        comp_key = NULL;
        for (i = 0; i < num_writes; ++i) {
                if (writeset[i].key == key && writeset[i].tableId == table_id) {
                        comp_key = &writeset[i];
                        break;
                }                
        }
        assert(comp_key != NULL);
        if (comp_key->is_initialized == false) {
                record = this->record_alloc->GetRecord(table_id);
                comp_key->is_initialized = true;
                comp_key->value = record;
                if (writeset[i].is_rmw == true) {
                        tid = stable_copy(key, table_id, record);
                        comp_key->old_tid = tid;
                }
        } 
        return RECORD_VALUE_PTR(comp_key->value);
}

int OCCAction::rand()
{
        return worker->gen_random();
}

void* OCCAction::read(uint64_t key, uint32_t table_id)
{
        uint64_t tid;
        void *record;
        uint32_t i, num_reads;
        occ_composite_key *comp_key;


        num_reads = this->readset.size();
        comp_key = NULL;
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].key == key &&
                    this->readset[i].tableId == table_id) {
                        comp_key = &this->readset[i];
                        break;
                }                
        }
        assert(comp_key != NULL);
        if (comp_key->is_initialized == false) {
                record = this->record_alloc->GetRecord(table_id);
                comp_key->is_initialized = true;
                comp_key->value = record;
                tid = stable_copy(key, table_id, record);
                comp_key->old_tid = tid;
        }        
        return RECORD_VALUE_PTR(comp_key->value);
}

void OCCAction::acquire_locks()
{
        uint32_t i, num_writes, table_id;
        uint64_t key;
        void *value;

        num_writes = this->writeset.size();
        std::sort(this->writeset.begin(), this->writeset.end());
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == false);
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;
                value = this->tables[table_id]->GetAlways(key);
                acquire_single((volatile uint64_t*)value);
                this->writeset[i].is_locked = true;
        }
}

void OCCAction::release_locks()
{
        uint32_t i, num_writes, table_id;
        uint64_t key;
        void *value;

        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == true);
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;
                value = this->tables[table_id]->Get(key);
                release_single((volatile uint64_t*)value);
                this->writeset[i].is_locked = false;
        }
}

void OCCAction::cleanup_single(occ_composite_key &comp_key)
{
        //        assert(comp_key.value != NULL);
        this->record_alloc->ReturnRecord(comp_key.tableId, comp_key.value);
        comp_key.value = NULL;
        comp_key.is_initialized = false;        
}

uint64_t OCCAction::compute_tid(uint32_t epoch, uint64_t last_tid)
{
        uint64_t max_tid, cur_tid, key;
        uint32_t num_reads, num_writes, i, table_id;
        volatile uint64_t *value;
        max_tid = CREATE_TID(epoch, 0);
        if (max_tid <  last_tid)
                max_tid = last_tid;
        assert(!IS_LOCKED(max_tid));
        num_reads = this->readset.size();
        num_writes = this->writeset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_tid = GET_TIMESTAMP(this->readset[i].old_tid);
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        for (i = 0; i < num_writes; ++i) {
                table_id = this->writeset[i].tableId;
                key = this->writeset[i].key;                
                value = (volatile uint64_t*)this->tables[table_id]->Get(key);
                assert(IS_LOCKED(*value));
                barrier();
                cur_tid = GET_TIMESTAMP(*value);
                barrier();
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        max_tid += 0x10;
        this->tid = max_tid;
        assert(!IS_LOCKED(max_tid));
        return max_tid;
}

bool OCCAction::run()
{
        return this->t->Run();
}

void OCCAction::cleanup()
{
        uint32_t i, num_writes, num_reads;
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(this->writeset[i].is_locked == false);
                if (this->writeset[i].is_initialized == true) 
                        cleanup_single(this->writeset[i]);
        }
        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].is_initialized == true) 
                        cleanup_single(this->readset[i]);
        }
}

void OCCAction::install_single_write(occ_composite_key &comp_key)
{
        assert(IS_LOCKED(this->tid) == false);

        void *value;
        uint64_t old_tid;
        uint32_t record_size;

        record_size = this->tables[comp_key.tableId]->RecordSize();
        value = this->tables[comp_key.tableId]->GetAlways(comp_key.key);
        old_tid = *(uint64_t*)value;
        assert(IS_LOCKED(old_tid) == true);
        memcpy(RECORD_VALUE_PTR(value), RECORD_VALUE_PTR(comp_key.value),
               record_size - sizeof(uint64_t));
        xchgq((volatile uint64_t*)value, this->tid);
        comp_key.is_locked = false;
}

void OCCAction::install_writes()
{
        uint32_t i, num_writes;
        num_writes = this->writeset.size();
        for (i = 0; i < num_writes; ++i) 
                install_single_write(this->writeset[i]);        
}
