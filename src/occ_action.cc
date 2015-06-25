#include <occ_action.h>

occ_composite_key::occ_composite_key(uint32_t table_id, uint64_t key,
                                     bool is_rmw)
{
        this->tableId = table_id;
        this->key = key;
        this->is_rmw = is_rmw;
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

void OCCAction::AddReadKey(uint32_t tableId, uint64_t key, bool is_rmw) 
{
        occ_composite_key k(tableId, key, is_rmw);
        readset.push_back(k);
}
        
void OCCAction::AddWriteKey(uint32_t tableId, uint64_t key)
{
        occ_composite_key k(tableId, key, false);
        writeset.push_back(k);
        shadow_writeset.push_back(k);
}

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
        /*
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
        */
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
        /*
        uint32_t i, num_fields;
        num_fields = recordSize/sizeof(uint64_t);
        __total = 0;
        for (i = 0; i < num_fields; ++i) 
                __total += __accumulated[i];
        */
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

uint64_t OCCAction::stable_copy(uint64_t key, uint32_t table_id, void *record)
{
        volatile uint64_t *tid_ptr;
        uint32_t record_size;
        uint64_t ret;
        void *value;

        value = this->tables[table_id]->Get(key);
        record_size = this->tables[table_id]->RecordSize();
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                ret = *tid_ptr;
                barrier();
                if (!IS_LOCKED(ret)) {
                        memcpy(RECORD_VALUE_PTR(record),
                               RECORD_VALUE_PTR(value), record_size);
                        return ret;
                }
        }
}

uint64_t OCCAction::stable_ref(uint64_t key, uint32_t table_id, void **ret)
{
        uint64_t tid;
        volatile uint64_t *tid_ptr;
        
        value = this->tables[table_id]->Get(key);
        *ret = value;
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                tid = *tid_ptr;
                barrier();
                if (!IS_LOCKED(tid)) 
                        return tid;
        }
}

bool OCCAction::validate()
{

}

void* OCCAction::write_ref(uint64_t key, uint32_t table_id)
{
        uint64_t tid;
        void *record;
        uint32_t i, num_writes;
        occ_composite_key *comp_key;
        
        /* 
         * 1) Get a record from record buffer. 
         * 2) Add the record.
         * 3) If this is an RMW, copy the value of the record.
         * 4) Return the record.
         */

        record = this->record_alloc->GetRecord(table_id);
        num_writes = this->writeset.size();
        comp_key = NULL;
        for (i = 0; i < num_writes; ++i) {
                if (writeset[i].key == key && writeset[i].tableId == table_id) {
                        comp_key = &writeset[i];
                        break;
                }                
        }
        assert(comp_key != NULL);
        if (writeset[i].is_rmw == true) {
                tid = stable_copy(key, table_id, record);
                comp_key->old_tid = tid;
        }
        comp_key->value = record;
        return RECORD_VALUE_PTR(record);
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
                if (readset[i].key == key && [i].tableId == table_id)
                        comp_key = &readset[i];
        }

        tid = stable_ref(key, table_id, &value);
        return RECORD_VALUE_PTR(value);                
}
