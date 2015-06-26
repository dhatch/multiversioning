#include <occ.h>
#include <action.h>
#include <cpuinfo.h>
#include <algorithm>

OCCWorker::OCCWorker(OCCWorkerConfig conf, struct RecordBuffersConfig rb_conf)
        : Runnable(conf.cpu)
{
        this->config = conf;
        this->bufs = new(conf.cpu) RecordBuffers(rb_conf);
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
        volatile uint32_t epoch;

        action->set_tables(this->config.tables);
        action->set_allocator(this->bufs);
        while (true) {
                try {
                        action->run();
                        action->acquire_locks();
                        barrier();
                        epoch = *config.epoch_ptr;
                        barrier();
                        
                        action->validate();
                        this->last_tid = action->compute_tid(epoch,
                                                             this->last_tid);
                        action->install_writes();
                        action->cleanup();
                        break;
                        
                } catch(const occ_validation_exception &e) {
                        assert(false);
                        action->release_locks();
                        action->cleanup();
                }
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
