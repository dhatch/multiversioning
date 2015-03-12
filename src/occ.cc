#include <occ.h>
#include <action.h>
#include <cpuinfo.h>
#include <algorithm>

OCCWorker::OCCWorker(OCCWorkerConfig conf, struct RecordBuffersConfig rb_conf)
        : Runnable(conf.cpu)
{
        this->config = conf;
        this->bufs = new(conf.cpu) RecordBuffers(rb_conf);
        this->logs[0] = (char*)alloc_mem(config.log_size, config.cpu);
        this->logs[1] = (char*)alloc_mem(config.log_size, config.cpu);
        assert(this->logs[0] != NULL && this->logs[1] != NULL);
        memset(this->logs[0], 0x0, config.log_size);
        memset(this->logs[1], 0x0, config.log_size);
        this->cur_log = 0;
        this->log_tail = this->logs[0];
}

void OCCWorker::Init()
{
}

/*
 * Process batches of transactions.
 */
void OCCWorker::StartWorking()
{
        if (config.cpu == 0) {
                EpochManager();
        } else {
                TxnRunner();
        }
}

void OCCWorker::EpochManager()
{
        uint64_t i, iters;
        iters = 100000;
        assert(iters < (config.epoch_threshold/2));
        barrier();
        incr_timestamp = rdtsc();
        barrier();
        while (true) {
                for (i = 0; i < iters; ++i) {
                        single_work();
                }
                UpdateEpoch();
        }
}

void OCCWorker::TxnRunner()
{
        uint32_t i;
        OCCActionBatch input, output;
        
        output.batch = NULL;
        while (true) {
                barrier();
                config.num_completed = 0;
                barrier();
                input = config.inputQueue->DequeueBlocking();
                for (i = 0; i < input.batchSize; ++i) {
                        RunSingle(input.batch[i]);
                }
                output.batchSize = config.num_completed;
                config.outputQueue->EnqueueBlocking(output);
        }
}

void OCCWorker::UpdateEpoch()
{
        uint32_t temp;
        barrier();
        volatile uint64_t now = rdtsc();
        barrier();
        if (now - incr_timestamp > config.epoch_threshold) {                
                temp = fetch_and_increment_32(config.epoch_ptr);                
                incr_timestamp = now;
                assert(temp != 0);
        }
}

uint64_t OCCWorker::NumCompleted()
{
        return config.num_completed;
}

/*
 * Run the action to completion. If the transaction aborts due to a conflict, 
 * retry.
 */
void OCCWorker::RunSingle(OCCAction *action)
{
        uint64_t cur_tid;
        occ_txn_status status;
        volatile uint32_t epoch;
        bool reset_log = false;
        PrepareWrites(action);
        PrepareReads(action);
        
        while (true) {
                status = action->Run();
                if (status.validation_pass == false) {
                        continue;
                }
                AcquireWriteLocks(action);
                barrier();
                epoch = *config.epoch_ptr;
                barrier();
                if (Validate(action)) {
                        reset_log = (epoch > GET_EPOCH(last_tid));
                        cur_tid = ComputeTID(action, epoch);
                        
                        InstallWrites(action, cur_tid);
                        fetch_and_increment(&config.num_completed);
                        Serialize(action, cur_tid, reset_log);
                        break;
                } else {
                        ReleaseWriteLocks(action);
                }
        }
        RecycleBufs(action);
}

void OCCWorker::SerializeSingle(const occ_composite_key &occ_key, uint64_t tid)
{
        uint32_t table_id, record_size;
        uint64_t key;
        uint64_t total_sz;
        struct occ_log_header header;
        char *log_head;

        log_head = logs[cur_log];
        assert(occ_key.GetValue() != NULL);
        table_id = occ_key.tableId;
        assert(table_id < config.num_tables);
        key = occ_key.key;
        record_size = config.tables[table_id]->RecordSize() - sizeof(uint64_t);
        header = {
                table_id,
                key,
                tid,
                record_size,
        };
        total_sz = sizeof(struct occ_log_header) + record_size;
        assert((uint64_t)(log_tail - log_head) + total_sz < config.log_size);
        memcpy(log_tail, &header, sizeof(struct occ_log_header));
        log_tail += sizeof(struct occ_log_header);
        memcpy(log_tail, occ_key.GetValue(), record_size);
        log_tail += record_size;
}

void OCCWorker::Serialize(OCCAction *action, uint64_t tid, bool reset)
{
        if (reset) {
                cur_log = (cur_log + 1) % 2;
                log_tail = logs[cur_log];
        }
        uint32_t i, num_writes;
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) 
                SerializeSingle(action->writeset[i], tid);
}

/*
 * The TID exceeds the existing TIDs of every record in the readset, writeset, 
 * and epoch.
 */
uint64_t OCCWorker::ComputeTID(OCCAction *action, uint32_t epoch)
{
        uint64_t max_tid, cur_tid, key;
        uint32_t num_reads, num_writes, i, table_id;
        volatile uint64_t *value;
        max_tid = CREATE_TID(epoch, 0);
        if (max_tid <  last_tid)
                max_tid = last_tid;
        assert(!IS_LOCKED(max_tid));
        num_reads = action->readset.size();
        num_writes = action->writeset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_tid = GET_TIMESTAMP(action->readset[i].old_tid);
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        for (i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                key = action->writeset[i].key;                
                value = (volatile uint64_t*)config.tables[table_id]->Get(key);
                assert(IS_LOCKED(*value));
                barrier();
                cur_tid = GET_TIMESTAMP(*value);
                barrier();
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        max_tid += 0x10;
        last_tid = max_tid;
        assert(!IS_LOCKED(max_tid));
        return max_tid;
}

/*
 * Remember the TID of every record in the readset. Used for validation.
 */
void OCCWorker::ObtainTIDs(OCCAction *action)
{
        uint32_t num_reads, i;
        volatile uint64_t *tid_ptr;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) {
                tid_ptr = (volatile uint64_t*)action->readset[i].value;
                barrier();
                action->readset[i].old_tid = *tid_ptr;
                barrier();
        }
}

/*
 * Obtain a reference to each record in the readset. Remember the TID and keep a
 * reference for each record.
 */
void OCCWorker::PrepareReads(OCCAction *action)
{
        uint32_t num_reads, table_id;
        uint64_t key;
        void *value;
        
        num_reads = action->readset.size();
        for (uint32_t i = 0; i < num_reads; ++i) {
                table_id = action->readset[i].tableId;
                key = action->readset[i].key;
                value = config.tables[table_id]->Get(key);
                action->readset[i].value = value;
        }
}

/*
 * 
 */
void OCCWorker::InstallWrites(OCCAction *action, uint64_t tid)
{
        //        assert(action->writeset.size() == action->write_records.size());
        assert(!IS_LOCKED(tid));
        uint32_t record_size, table_id;
        uint64_t key;
        void *value;
        for (uint32_t i = 0; i < action->writeset.size(); ++i) {
                table_id = action->writeset[i].tableId;
                key = action->writeset[i].key;
                value = (void*)config.tables[table_id]->Get(key);
                assert(IS_LOCKED(*(uint64_t*)value) == true);
                assert(GET_TIMESTAMP(*(uint64_t*)value) < tid);
                record_size = config.tables[table_id]->RecordSize() - sizeof(uint64_t);
                memcpy(RECORD_VALUE_PTR(value), action->writeset[i].GetValue(),
                       record_size);
                barrier();
//                barrier();
//                *RECORD_TID_PTR(value) = tid;
//                barrier();
                xchgq(RECORD_TID_PTR(value), tid);
        }
}

/*
 *
 */
void OCCWorker::PrepareWrites(OCCAction *action)
{
        uint32_t num_writes, table_id;
        void *rec;
                
        num_writes = action->writeset.size();
        for (uint32_t i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                rec = bufs->GetRecord(table_id);
                action->writeset[i].value = rec;
        }
}

/*
 *
 */
void OCCWorker::RecycleBufs(OCCAction *action)
{
        uint32_t num_writes, table_id;
        void *rec;
        num_writes = action->writeset.size();
        for (uint32_t i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                rec = (void*)action->writeset[i].value;
                bufs->ReturnRecord(table_id, rec);
        }
}

/*
 * Silo's validation protocol.
 */
bool OCCWorker::Validate(OCCAction *action)
{
        uint32_t num_reads, i;
        num_reads = action->readset.size();
        barrier();
        for (i = 0; i < num_reads; ++i) 
                if (!action->readset[i].ValidateRead())
                        return false;
        barrier();
        return true;
}

/*
 * Try to acquire a record's write latch.
 */
inline bool OCCWorker::TryAcquireLock(volatile uint64_t *version_ptr)
{
        volatile uint64_t cmp_tid, locked_tid;
        barrier();
        cmp_tid = *version_ptr;
        barrier();
        if (IS_LOCKED(cmp_tid))
                return false;        
        locked_tid = (cmp_tid | 1);
        return cmp_and_swap(version_ptr, cmp_tid, locked_tid);
}

/*
 * Acquire write lock for a single record. Exponential back-off under 
 * contention.
 */
bool OCCWorker::AcquireSingleLock(volatile uint64_t *version_ptr)
{
        bool waited = false;
        uint32_t backoff, temp;
        if (USE_BACKOFF) 
                backoff = 1;
        while (true) {
                if (TryAcquireLock(version_ptr)) {
                        assert(IS_LOCKED(*version_ptr));
                        break;
                }
                if (USE_BACKOFF) {
                        temp = backoff;
                        while (temp-- > 0)
                                single_work();
                        backoff = backoff*2;
                }
                waited = true;
        }
        return waited;
}

/*
 * Acquire a lock for every record in the transaction's writeset.
 */
bool OCCWorker::AcquireWriteLocks(OCCAction *action)
{
        bool waited;
        uint32_t num_writes, i, table_id;
        uint64_t key;
        volatile uint64_t *tid_ptr;
        waited = false;
        num_writes = action->writeset.size();
        std::sort(action->writeset.begin(), action->writeset.end());
        for (i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                key = action->writeset[i].key;
                tid_ptr = (volatile uint64_t*)config.tables[table_id]->Get(key);
                waited |= AcquireSingleLock(tid_ptr);
                assert(IS_LOCKED(*tid_ptr));
        }
        return waited;
}

/*
 * Release write locks by zero-ing the least significant 4 bits.
 */
void OCCWorker::ReleaseWriteLocks(OCCAction *action)
{
        uint32_t num_writes, i, table_id;
        volatile uint64_t *tid_ptr;
        uint64_t old_tid, key;
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                key = action->writeset[i].key;
                tid_ptr = (volatile uint64_t*)config.tables[table_id]->Get(key);
                barrier();
                old_tid = *tid_ptr;
                barrier();
                assert(IS_LOCKED(old_tid));
                old_tid = GET_TIMESTAMP(old_tid);
                assert(!IS_LOCKED(old_tid));
                //                assert(GET_TIMESTAMP(old_tid) == GET_TIMESTAMP(*tid_ptr));
                xchgq(tid_ptr, old_tid);
//                barrier();
//                *tid_ptr = old_tid;
//                barrier();
        }
}

/*
 * Create a linked list of thread local buffers for a particular type of record.
 * Given the size of each buffer and the number of buffers.
 */
void RecordBuffers::LinkBufs(struct RecordBuffy *start, uint32_t buf_size,
                             uint32_t num_bufs)
{
        uint32_t offset, i;
        char *cur;
        struct RecordBuffy *temp;

        assert(num_bufs > 0);
        offset = sizeof(struct RecordBuffy*) + buf_size;
        cur = (char*)start;
        for (i = 0; i < num_bufs; ++i) {
                temp  = (struct RecordBuffy*)cur;
                temp->next = (struct RecordBuffy*)(cur + offset);
                cur = (char*)(temp->next);
        }
        temp->next = NULL;
}

void* RecordBuffers::AllocBufs(struct RecordBuffersConfig conf)
{
        uint32_t i;
        uint64_t total_size, single_buf_sz;
        total_size = 0;
        for (i = 0; i < conf.num_tables; ++i) {
                single_buf_sz =
                        sizeof(struct RecordBuffy*)+conf.record_sizes[i];
                total_size += conf.num_buffers * single_buf_sz;
        }
        //        std::cerr << "Record size: " << total_size << "\n";
        return alloc_mem(total_size, conf.cpu);
}

/*
 * 
 */
RecordBuffers::RecordBuffers(struct RecordBuffersConfig conf)
{
        uint32_t i;
        char *temp;
        temp = (char *)alloc_mem(conf.num_tables*sizeof(struct RecordBuffy*),
                                 conf.cpu);
        assert(temp != NULL);
        record_lists = (struct RecordBuffy**)temp;
        temp = (char*)AllocBufs(conf);
        assert(temp != NULL);
        for (i = 0; i < conf.num_tables; ++i) {
                LinkBufs((struct RecordBuffy*)temp, conf.record_sizes[i],
                         conf.num_buffers);
                record_lists[i] = (struct RecordBuffy*)temp;
                temp += conf.record_sizes[i]*conf.num_buffers;
        }
}

void* RecordBuffers::GetRecord(uint32_t tableId)
{
        RecordBuffy *ret;
        assert(record_lists[tableId] != NULL);
        ret = record_lists[tableId];
        record_lists[tableId] = ret->next;
        ret->next = NULL;
        return ret;
}

void RecordBuffers::ReturnRecord(uint32_t tableId, void *record)
{
        RecordBuffy *ret;
        ret = (RecordBuffy*)record;
        ret->next = record_lists[tableId];
        record_lists[tableId] = ret;
}
