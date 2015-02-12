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
        uint64_t *temp;
        temp = (uint64_t*)value;
        tid_ptr = (volatile uint64_t*)value;
        while (true) {
                tid = *tid_ptr;
                if (!IS_LOCKED(tid))
                        break;
        }
        this->old_tid = tid;
        return &temp[1];
}

bool occ_composite_key::FinishRead()
{
        volatile uint64_t tid;
        bool is_valid = false;
        barrier();        
        tid = *(volatile uint64_t*)value;
        barrier();
        is_valid = (tid == this->old_tid);
        return is_valid;
}

uint64_t occ_composite_key::GetTimestamp()
{
        return old_tid;
}

bool occ_composite_key::ValidateRead()
{        
        volatile uint64_t *version_ptr = (volatile uint64_t*)value;
        barrier();
        volatile uint64_t ver = *version_ptr;
        barrier();
        if ((GET_TIMESTAMP(ver) != GET_TIMESTAMP(old_tid)) ||
            (IS_LOCKED(ver) && !is_rmw))
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

bool RMWOCCAction::DoReads()
{
        bool success = true;
        uint32_t num_fields, num_reads, i, j;
        uint64_t *field_ptr, counter;
        counter = 0;
        num_fields = recordSize/sizeof(uint64_t);
        num_reads = readset.size();
        for (i = 0; success == true && i < num_reads; ++i) {
                field_ptr = (uint64_t*)readset[i].StartRead();
                for (j = 0; j < num_fields; ++j)
                        counter += field_ptr[j];
                success = readset[i].FinishRead();
        }
        __total = counter;
        return success;                
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
        uint32_t i, num_writes;
        uint64_t *field_ptr;
        counter = __total;
        num_writes = writeset.size();
        for (i = 0; i < num_writes; ++i) {
                field_ptr = (uint64_t*)writeset[i].GetValue();
                field_ptr[i] = counter+i;
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
        if (DoReads() == false) 
                return status;
        else
                status.validation_pass = true;                
        AccumulateValues();
        DoWrites();
        return status;
}
