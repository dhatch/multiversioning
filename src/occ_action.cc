#include <occ_action.h>

occ_composite_key::occ_composite_key(uint32_t table_id, uint64_t key,
                                     bool is_rmw)
{
        this->tableId = table_id;
        this->key = key;
        this->is_rmw = is_rmw;
}

void* occ_composite_key::GetValue()
{
        uint64_t *temp = (uint64_t*)value;
        return &temp[1];
}

void* occ_composite_key::StartRead()
{
        volatile uint64_t *tid_ptr, tid;
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                barrier();
                tid = *tid_ptr;
                barrier();
                if (!IS_LOCKED(tid))
                        break;
        }
        this->old_tid = tid;
        return (void*)&tid_ptr[1];
}

bool occ_composite_key::FinishRead()
{
        volatile uint64_t tid;
        bool is_valid = false;
        barrier();        
        tid = *(volatile uint64_t*)value;
        barrier();
        is_valid = (tid == this->old_tid);
        assert(!is_valid || !IS_LOCKED(tid));
        return is_valid;
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
                        uint32_t *write_p = (uint32_t*)&__reads[j*100];
                        *write_p += *((uint32_t*)&read_ptr[j*100]);
                }
                if (readset[i].FinishRead() == false) {
                        status.validation_pass = false;
                        break;
                }                
        }
        return status;        
}

bool RMWOCCAction::DoReads()
{
        uint32_t num_fields, num_reads, i, j, field_sz;
        char *read_ptr, *write_ptr;
        num_fields = 10;
        field_sz = 100;
        num_reads = readset.size();
        for (i = 0; i < num_reads; ++i) {
                read_ptr = (char*)readset[i].StartRead();
                write_ptr = (char*)writeset[i].GetValue();
                memcpy(write_ptr, read_ptr, 1000);
                for (j = 0; j < num_fields; ++j) {
                        uint32_t *temp_ptr = (uint32_t*)&write_ptr[j*field_sz];
                        *temp_ptr += j+1;
                }
                if (readset[i].FinishRead() == false) 
                        return false;
        }
        //        __total = counter;
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
