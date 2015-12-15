#include <mv_action.h>
#include <table.h>
#include <executor.h>

extern Table** mv_tables;

Action::Action()
{
        this->__version = 0;
        this->__combinedHash = 0;
        this->__readonly = false;
        this->__state = STICKY;
        for (uint32_t i = 0; i < NUM_CC_THREADS; ++i) {
                this->__write_starts.push_back(-1);
                this->__read_starts.push_back(-1);
        }
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
        //        uint64_t key = __readset[index].key;
        //        return mv_tables[0]->Get(key);
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
        //        uint64_t key = __writeset[index].key;
        //        return mv_tables[0]->Get(key);
        MVRecord *record = __writeset[index].value;
        assert(record->value != NULL);
        return record->value;
}

void* Action::ReadWrite(uint32_t index)
{
        //        uint64_t key = __writeset[index].key;
        //        return mv_tables[0]->Get(key);
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

InsertAction::InsertAction() : Action()
{
}

bool InsertAction::Run()
{
        /*
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
        */
        return true;
}

mv_readonly::mv_readonly()
{
        __readonly = true;
}

bool mv_readonly::Run()
{
        uint32_t i, j, num_reads;
        assert(__readonly == true);
        num_reads = __readset.size();
        for (i = 0; i < num_reads; ++i) {
                char *read_ptr = (char*)Read(i);
                for (j = 0; j < 10; ++j) {
                        uint64_t *write_p = (uint64_t*)&__reads[j*100];
                        *write_p += *((uint64_t*)&read_ptr[j*100]);
                }                
        }
        
        return true;
}

bool mv_mix_action::Run()
{
        uint32_t i, j, num_reads, num_writes;
        num_writes = __writeset.size();
        num_reads = __readset.size();
        for (i = 0; i < num_reads; ++i) {
                char *read_ptr = (char*)Read(i);
                for (j = 0; j < 10; ++j) {
                        uint32_t *write_p = (uint32_t*)&__reads[j*100];
                        *write_p += *((uint32_t*)&read_ptr[j*100]);
                }                
        }
        for (i = 0; i < num_writes; ++i) {
                if (__writeset[i].is_rmw == true) {
                        char *read_ptr = (char*)ReadWrite(i);
                        for (j = 0; j < 10; ++j) {
                                uint32_t *write_p = (uint32_t*)&__reads[j*100];
                                *write_p += *((uint32_t*)&read_ptr[j*100]);
                        }                
                } else {
                        break;
                }
        }
        for (i = 0; i < num_writes; ++i) {
                char *ptr = (char*)GetWriteRef(i);
                memcpy(ptr, __reads, 1000);
                for (j = 0; j < 10; ++j) {
                        uint32_t *write_p = (uint32_t*)&ptr[j*100];
                        *write_p += i+j;
                }
        }
        return true;
}

RMWAction::RMWAction(uint64_t seed) : Action()
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
        char *field_ptr;
        num_fields = 10;
        num_writes = __writeset.size();
        for (i = 0; i < num_writes; ++i) {
                assert(__writeset[i].is_rmw == true);
                memcpy(GetWriteRef(i), ReadWrite(i), 1000);
                field_ptr = (char*)GetWriteRef(i);
                for (j = 0; j < num_fields; ++j) {
                        *((uint32_t*)&field_ptr[j*100]) += j+1;
                }
        }
}

bool RMWAction::Run()
{
        uint32_t i, j, num_reads, num_writes, num_fields;
        assert(recordSize == 1000);
        num_reads = __readset.size();
        num_writes = __writeset.size();
        num_fields = YCSB_RECORD_SIZE / 100;
        uint64_t counter = 0;
        for (i = 0; i < num_reads; ++i) {
                char *field_ptr = (char*)Read(i);
                if (SMALL_RECORDS) {
                        counter += *((uint64_t*)field_ptr);
                } else {
                        for (j = 0; j < num_fields; ++j) 
                                counter += *((uint64_t*)&field_ptr[j*100]);
                }
        }
        for (i = 0; i < num_writes; ++i) {
                if (__writeset[i].is_rmw) {
                        char *field_ptr = (char*)ReadWrite(i);
                        if (SMALL_RECORDS) {
                                counter += *((uint64_t*)field_ptr);
                        } else {
                                for (j = 0; j < num_fields; ++j)
                                        counter += *((uint64_t*)&field_ptr[j*100]);
                        }
                }
        }

        for (i = 0; i < num_writes; ++i) {
                assert(__writeset[i].is_rmw);
                char *read_ptr = (char*)ReadWrite(i);
                char *write_ptr = (char*)GetWriteRef(i);
                if (SMALL_RECORDS) {
                        *((uint64_t*)write_ptr) =
                                counter + *((uint64_t*)read_ptr);
                } else {
                        memcpy(write_ptr, read_ptr, YCSB_RECORD_SIZE);
                        for (j = 0; j < num_fields; ++j)
                                *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
                        //              *((uint64_t*)&read_ptr[j*100]);
                }
        }

        return true;
}

mv_action::mv_action(txn *t) : translator(t)
{
        this->__version = 0;
        this->__combinedHash = 0;
        this->__readonly = false;
        this->__state = STICKY;
        for (uint32_t i = 0; i < NUM_CC_THREADS; ++i) {
                this->__write_starts.push_back(-1);
                this->__read_starts.push_back(-1);
        }
        this->init = false;
        this->read_index = 0;
        this->write_index = 0;
}

bool mv_action::initialized()
{
        return init;
}

static struct big_key get_key(CompositeKey k)
{
        struct big_key ret;
        ret.key = k.key;
        ret.table_id = k.tableId;
        return ret;
}

/*
 * Assumes that all the entries in the transaction's read- and write-sets are 
 * initialized.
 */
void mv_action::setup_reverse_index()
{
        uint32_t num_reads, num_writes, i;
        struct big_key key;
        struct key_index index;

        assert(init == false);
        num_reads = __readset.size();
        num_writes = __writeset.size();

        /* Initialize reverse index to items in the write-set. */
        for (i = 0; i < num_writes; ++i) {
                key = get_key(__writeset[i]);
                index.index = i;
                index.initialized = false;
                if (__writeset[i].is_rmw == true)
                        index.use = RMW;
                else
                        index.use = WRITE;
                assert(reverse_index.count(key) == 0);
                reverse_index[key] = index;
        }

        /* Initialize reverse index to items in the read-set. */
        for (i = 0; i < num_reads; ++i) {
                key = get_key(__readset[i]);
                index.index = i;
                index.initialized = true;
                index.use = READ;
                assert(reverse_index.count(key) == 0);
                reverse_index[key] = index;
        }

        /* Optimize read-only txns in the execution phase. */
        if (num_writes == 0)
                __readonly = true;
        else
                __readonly = false;
        init = true;
}

bool mv_action::Run()
{
        return t->Run();
}

void* mv_action::write_ref(uint64_t key, uint32_t table_id)
{
        //        struct big_key bkey;
        //        struct key_index index;
        uint32_t num_writes, i;
        
        assert(init == true);
        num_writes = this->__writeset.size();
        //        assert(num_writes < 10);
        for (i = 0; i < num_writes; ++i) {
                if (this->__writeset[i].key == key &&
                    this->__writeset[i].tableId == table_id) {
                        assert(!this->__writeset[i].is_rmw || 
                               this->__writeset[i].initialized == true);
                        return this->__writeset[i].value->value;
                }
        }
        assert(false);
        return NULL;
}

void* mv_action::read(uint64_t key, uint32_t table_id)
{
        //        struct big_key bkey;
        //        struct key_index index;
        MVRecord *record, *snapshot;
        uint32_t i, num_reads;
        void *ret;
        
        assert(init == true);
        record = NULL;
        num_reads = this->__readset.size();
        //        assert(num_reads < 10);
        for (i = 0; i < num_reads; ++i) {
                if (this->__readset[i].key == key &&
                    this->__readset[i].tableId == table_id) {
                        record = this->__readset[i].value;
                        break;
                }
        }
        assert(record != NULL);
        if (this->__readonly == true &&
            (
             GET_MV_EPOCH(this->__version) ==
             GET_MV_EPOCH(record->createTimestamp)
             )) {
                snapshot = record->epoch_ancestor;
                ret = (void*)snapshot->value;
        } else {
                ret = (void*)record->value;
        }
        return ret;

        /*
        bkey.key = key;
        bkey.table_id = table_id;
        assert(reverse_index.count(bkey) == 1);
        index = reverse_index[bkey];
        assert(index.use == READ || index.use == RMW);
        if (index.use == READ) {
                record = __readset[index.index].value;
                if (__readonly == true &&
                    (
                     GET_MV_EPOCH(__version) ==
                     GET_MV_EPOCH(record->createTimestamp)
                     )) {
                        snapshot = record->epoch_ancestor;
                        ret = (void*)snapshot->value;
                } else {
                        ret = (void*)__readset[index.index].value->value;
                }
        } else {	// index.use == RMW
                assert(__readonly == false);
                ret = __writeset[index.index].value->value;
        }
        return ret;
        */
}

CompositeKey mv_action::GenerateKey(bool is_rmw, uint32_t tableId, uint64_t key)
{
        CompositeKey toAdd(is_rmw, tableId, key);
        uint32_t threadId =
                CompositeKey::HashKey(&toAdd) % NUM_CC_THREADS;
        toAdd.threadId = threadId;
        this->__combinedHash |= ((uint64_t)1) << threadId;
        return toAdd;
}


void mv_action::add_read_key(uint32_t tableId, uint64_t key)
{
        CompositeKey to_add;
        assert(tableId == 0 || tableId == 1);
        to_add = GenerateKey(false, tableId, key);
        __readset.push_back(to_add);
}

void mv_action::add_write_key(uint32_t tableId, uint64_t key, bool is_rmw)
{
        CompositeKey to_add;
        assert(tableId == 0 || tableId == 1);
        to_add = GenerateKey(is_rmw, tableId, key);
        __writeset.push_back(to_add);
        __readonly = false;
}

int mv_action::rand()
{
        return exec->gen_random();
}
