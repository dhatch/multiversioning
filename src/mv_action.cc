#include <mv_action.h>

Action::Action(uint64_t version)
{
        this->__version = version;
        this->__combinedHash = 0;
        this->__readonly = false;
        this->__state = STICKY;
}

CompositeKey Action::GenerateKey(uint32_t tableId, uint64_t key)
{
        CompositeKey toAdd(tableId, key);
        uint32_t threadId =
                CompositeKey::HashKey(&toAdd) % NUM_CC_THREADS;
        toAdd.threadId = threadId;
        this->combinedHash |= ((uint64_t)1) << threadId;
        return toAdd;
}

void* Action::Read(uint32_t index)
{
        MVRecord *record = __readset[index].value;
        if (__readonly == true &&
            GET_MV_EPOCH(__version) == GET_MV_EPOCH(record->createTimestamp)) {
                MVRecord *snapshot = record->epoch_ancestor;
                return (void*)snapshot->value;
        } else {
                return (void*)__readset[index].value->value;
        }
}

void* Action::GetWriteRef(uint32_t index)
{
        MVRecord *record = __writeset[index].value;
        assert(record->value != NULL);
        return record->value;
}

void* Action::ReadWrite(uint32_t index)
{
        MVRecord *record = __writeset[index].value;
        return (void*)record->recordLink->value;
}

bool InsertAction::Run()
{
        uint32_t num_writes, i, j;
        uint64_t *ref, key;
        if (recordSize == 8) {
                num_writes = __writeset.size();
                for (i = 0; i < num_writes; ++i) {
                        key = __writeset[i].key;
                        ref = (uint64_t*)GetWriteRef(i);
                        *ref = key;
                }
        } else if (recordSize == 1000) {
                num_writes = __writeset.size();
                for (i = 0; i < numWrites; ++i) {
                        ref = (uint64_t*)GetWriteRef(i);
                        for (j = 0; j < 125; ++j) 
                                ref[j] = (uint64_t)rand();
                        
                }
        } else {
                assert(false);
        }
        return true;
}



void RMWAction::DoReads()
{
        uint32_t num_fields, num_reads, i, j;
        uint64_t *field_ptr;
        num_fields = recordSize/sizeof(uint64_t);
        num_reads = __readset.size();
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (uint64_t*)__readset[i].GetValue();
                for (j = 0; j < num_fields; ++j) 
                        __accumulated[j] += field_ptr[j];
        }        

}

void RMWAction::AccumulateValues()
{
        uint32_t i, num_fields;
        num_fields = recordSize/sizeof(uint64_t);
        __total = 0;
        for (i = 0; i < num_fields; ++i) 
                __total += __accumulated[i];

}

void RMWAction::DoWrites()
{
        uint32_t i, num_writes;
        void *field_ptr;
        num_writes = __writeset.size();
        for (i = 0; i < num_writes; ++i) {
                field_ptr = __writeset[i].GetValue();
                memcpy(field_ptr, __accumulated, recordSize);
        }
}

bool RMWAction::Run()
{
        assert(recordSize == 1000);
        DoReads();
        AccumulateValues();
        DoWrites();
        return true;
}
