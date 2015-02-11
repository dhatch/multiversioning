#include <occ_action.h>

occ_composite_key::occ_composite_key(uint32_t table_id, uint64_t key,
                                     bool is_rmw)
{
        this->tableId = tableId;
        this->key = key;
        this->is_rmw = is_rmw;
}

void* occ_composite_key::GetValue()
{
        uint64_t *temp = (uint64_t*)value;
        return &temp[1];
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

void RMWOCCAction::DoReads()
{
        uint32_t num_fields, num_reads, i, j;
        uint64_t *field_ptr;
        num_fields = recordSize/sizeof(uint64_t);
        num_reads = readset.size();
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (uint64_t*)readset[i].GetValue();
                for (j = 0; j < num_fields; ++j) 
                        __accumulated[j] += field_ptr[j];
        }        
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
        uint32_t i, num_writes;
        void *field_ptr;
        num_writes = writeset.size();
        for (i = 0; i < num_writes; ++i) {
                field_ptr = writeset[i].GetValue();
                memcpy(field_ptr, __accumulated, recordSize);
        }
}

bool RMWOCCAction::Run()
{
        assert(recordSize == 1000);
        DoReads();
        AccumulateValues();
        DoWrites();
        return true;
}
