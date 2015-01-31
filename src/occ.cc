#include <occ.h>
#include <action.h>

void OCCWorker::Init()
{
}

/*
 * Process batches of transactions.
 */
void OCCWorker::StartWorking()
{
        OCCActionBatch input, output;
        input = config.inputQueue->DequeueBlocking();
        for (uint32_t i = 0; i < input.batchSize; ++i)
                RunSingle(input.batch[i]);
        config.outputQueue->EnqueueBlocking(input);
}

/*
 * Run the action to completion. If the transaction aborts due to a conflict, 
 * retry.
 */
void OCCWorker::RunSingle(OCCAction *action)
{
        uint64_t cur_tid;
        bool commit;
        PrepareWrites(action);
        PrepareReads(action);
        while (true) {
                action->ObtainTIDs();
                commit = action->Run();
                AcquireWriteLocks(action);
                if (Validate(action)) {
                        if (commit)
                                InstallWrites(action);
                        RecycleBufs(action);
                        break;
                } else {
                        ReleaseWriteLocks(action);
                }
        }
}

/*
 * The TID exceeds the existing TIDs of every record in the readset, writeset, 
 * and epoch.
 */
uint64_t OCCWorker::ComputeTID(OCCAction *action)
{
        uint64_t max_tid, cur_tid;
        uint32_t num_reads, num_writes, i;
        max_tid = CREATE_TID(action->epoch, 0);
        num_reads = action->readset.size();
        num_writes = action->writeset.size();
        for (i = 0; i < num_reads; ++i) {
                cur_tid = GET_TIMESTAMP(*action->readset[i].old_tid);
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        for (i = 0; i < num_writes; ++i) {
                assert(IS_LOCKED(*action->write_records[i]));
                cur_tid = GET_TIMESTAMP(*action->write_records[i]);
                assert(!IS_LOCKED(cur_tid));
                if (cur_tid > max_tid)
                        max_tid = cur_tid;
        }
        max_tid += 0x10;
        return max_tid;
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
        assert(action->writeset.size() == action->write_records.size());
        assert(!IS_LOCKED(tid));
        uint32_t record_size, table_id;
        uint64_t key;
        void *value;
        for (uint32_t i = 0; i < action->writeset.size(); ++i) {
                value = action->write_records[i];
                memset(RECORD_VALUE_PTR(value), action->writeset[i].GetValue(),
                       record_size);
                xchgq(RECORD_TID_PTR(value), tid);
        }
}

void OCCWorker::PrepareWrites(OCCAction *action)
{
        uint32_t num_writes, table_id;
        uint64_t key;
        void *rec;
        void *value;
        num_writes = action->writeset.size();
        for (uint32_t i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                key = action->writeset[i].key;
                rec = bufs->GetRecord(table_id);
                action->writeset[i].value = rec;
                value = config.tables[table_id]->Get(key);
                action->write_records[i] = value;
        }
}

void OCCWorker::RecycleBufs(OCCAction *action)
{
        uint32_t num_writes, table_id;
        void *rec;
        num_writes = action->writeset.size();
        for (uint32_t i = 0; i < num_writes; ++i) {
                table_id = action->writeset[i].tableId;
                rec = action->writeset[i].value;
                bufs->ReturnRecord(table_id, rec);
        }
}

void OCCWorker::LockWrites(OCCAction *action)
{
        uint32_t num_writes, i;
        for (i = 0; i < num_writes; ++i) {
                action->write_records[i].
        }
}

/*
 * Silo's validation protocol.
 */
bool OCCWorker::Validate(OCCAction *action)
{
        uint32_t num_reads, i;
        num_reads = action->readset.size();
        for (i = 0; i < num_reads; ++i) 
                if (!action->readset[i].ValidateRead())
                        return false;
        return true;
}

/*
 * Try to acquire a record's write latch.
 */
static inline bool OCCWorker::TryAcquireLock(volatile uint64_t *version_ptr)
{
        volatile uint64_t cmp_tid, locked_tid;
        cmp_tid = *version_ptr;
        locked_tid = (cmp_tid | 1);
        if (!IS_LOCKED(cmp_tid) &&
            cmp_and_swap(version_ptr, cmp_tid, locked_tid))
                return true;
        return false;
}

/*
 * Acquire write lock for a single record. Exponential back-off under 
 * contention.
 */
void OCCWorker::AcquireSingleLock(volatile uint64_t *version_ptr)
{
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
        }
}

/*
 * Acquire a lock for every record in the transaction's writeset.
 */
void OCCWorker::AcquireWriteLocks(OCCAction *action)
{
        uint32_t num_writes, i;
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) 
                AcquireSingleLock((volatile uint64_t*)action->write_records[i]);
}

/*
 * Release write locks by zero-ing the least significant 4 bits.
 */
void OCCWorker::ReleaseWriteLocks(OCCAction *action)
{
        uint32_t num_writes, i;
        volatile uint64_t *tid_ptr;
        uint64_t old_tid;
        num_writes = action->writeset.size();
        for (i = 0; i < num_writes; ++i) {
                tid_ptr = (volatile uint64_t*)action->write_records[i];
                assert(IS_LOCKED(*tid_ptr));
                old_tid = *tid_ptr & ~TIMESTAMP_MASK;
                assert(!IS_LOCKED(old_tid));
                assert(GET_TIMESTAMP(old_tid) == GET_TIMESTAMP(*tid_ptr));
                xchgq(tid_ptr, old_tid);
        }
}
