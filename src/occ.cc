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
        PrepareWrites(action);
        PrepareReads(action);
        while (true) {
                action->ObtainTIDs();
                if (!action->Run() || Validate(action)) {
                        InstallWrites(action);
                        RecycleBufs(action);
                        break;
                }
        }
}

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

void OCCWorker::InstallWrites(OCCAction *action, uint32_t epoch, uint32_t ts)
{
        
}

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

/*
 * Obtain refs to elems in the read set and allocate space for elems in the 
 * write set.
 */
void OCCWorker::Prepare(OCCAction *action)
{
        uint32_t num_reads, num_writes;
        uint32_t tableId;
        uint64_t key, old_tid;
        void *value;
        volatile uint64_t *tid_ptr;
        num_reads = action->readset.size();
        num_writes = action->writeset.size();
        for (uint32_t i = 0; i < num_reads; ++i) {
                tableId = action->readset[i].tableId;
                key = action->readset[i].key;
                value = config.tables[tableId]->Get(key);
                tid_ptr = (volatile uint64_t*)value;
                action->readset[i].old_tid = *tid_ptr;
        }
        for (uint32_t i = 0; i < num_writes; ++i) {
                
        }
}

/*
 * Silo's validation protocol.
 */
bool OCCWorker::Validate(OCCAction *action)
{
        
}
