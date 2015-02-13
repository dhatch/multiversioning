#include <mv_action.h>

Action::Action()
{
        this->__version = 0;
        this->__combinedHash = 0;
        this->__readonly = false;
        this->__state = STICKY;
}

CompositeKey Action::GenerateKey(bool is_rmw, uint32_t tableId, uint64_t key)
{
        CompositeKey toAdd(is_rmw, tableId, key);
        uint32_t threadId =
                CompositeKey::HashKey(&toAdd) % NUM_CC_THREADS;
        toAdd.threadId = threadId;
        this->__combinedHash |= ((uint64_t)1) << threadId;
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
        assert(__writeset[index].is_rmw);
        MVRecord *record = __writeset[index].value;
        return (void*)record->recordLink->value;
}

void Action::AddReadKey(uint32_t tableId, uint64_t key)
{
        CompositeKey to_add;
        to_add = GenerateKey(false, tableId, key);
        __readset.push_back(to_add);
}

void Action::AddWriteKey(uint32_t tableId, uint64_t key, bool is_rmw)
{
        CompositeKey to_add;
        to_add = GenerateKey(is_rmw, tableId, key);
        __writeset.push_back(to_add);
        __readonly = false;
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
                for (i = 0; i < num_writes; ++i) {
                        ref = (uint64_t*)GetWriteRef(i);
                        for (j = 0; j < 125; ++j) 
                                ref[j] = (uint64_t)rand();
                        
                }
        } else {
                assert(false);
        }
        return true;
}

RMWAction::RMWAction(uint64_t seed)
{
        __total = seed;
}

void RMWAction::DoReads()
{
        uint32_t num_fields, num_reads, num_writes, i, j;
        uint64_t *field_ptr, counter;
        counter = 0;
        num_fields = recordSize/sizeof(uint64_t);
        num_reads = __readset.size();
        num_writes = __writeset.size();
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (uint64_t*)Read(i);
                for (j = 0; j < num_fields; ++j)
                        counter += field_ptr[j];
        }        
        for (i = 0; i < num_writes; ++i) {
                if (__writeset[i].is_rmw == true) {
                        field_ptr = (uint64_t*)ReadWrite(i);
                        for (j = 0; j < num_fields; ++j)
                                counter += field_ptr[j];
                }
        }
        __total += counter;
}

void RMWAction::AccumulateValues()
{
        /*
        uint32_t i, num_fields;
        num_fields = recordSize/sizeof(uint64_t);
        __total = 0;
        for (i = 0; i < num_fields; ++i) 
                __total += __accumulated[i];
        */

}

void RMWAction::DoWrites()
{
        uint32_t i, j, num_writes, num_fields;
        uint64_t *field_ptr;
        uint64_t counter;
        num_fields = recordSize/sizeof(uint64_t);
        counter = __total;
        num_writes = __writeset.size();
        for (i = 0; i < num_writes; ++i) {                
                field_ptr = (uint64_t*)GetWriteRef(i);
                for (j = 0; j < num_fields; ++j)
                        field_ptr[j] = counter+j;
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
