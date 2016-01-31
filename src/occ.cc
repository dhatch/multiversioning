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

uint32_t OCCWorker::exec_pending(OCCAction **pending_list)
{
        OCCAction *cur, *prev;
        uint32_t num_done;

        prev = NULL;
        cur = *pending_list;
        num_done = 0;
        while (cur != NULL) {
                if (RunSingle(cur)) {
                        if (prev == NULL) 
                                *pending_list = cur->link;
                        else 
                                prev->link = cur->link;                        
                        num_done += 1;
                } else {
                        prev = cur;
                }                
                cur = cur->link;
        }
        return num_done;
}

void OCCWorker::TxnRunner()
{
        uint32_t i, num_pending;
        OCCActionBatch input, output;
        OCCAction *pending_list;
        
        num_pending = 0;
        pending_list = NULL;
        output.batch = NULL;
        while (true) {
                input = config.inputQueue->DequeueBlocking();
                for (i = 0; i < input.batchSize; ++i) {
                        while (num_pending >= 50) 
                                num_pending -= exec_pending(&pending_list);
                        if (!RunSingle(input.batch[i])) {
                                input.batch[i]->link = pending_list;
                                pending_list = input.batch[i];
                                num_pending += 1;
                        }
                }                
                while (num_pending != 0) 
                        num_pending -= exec_pending(&pending_list);
                assert(pending_list == NULL);
                output.batchSize = input.batchSize;
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
bool OCCWorker::RunSingle(OCCAction *action)
{
        volatile uint32_t epoch;
        bool validated;

        action->set_tables(this->config.tables);
        action->set_allocator(this->bufs);
        action->worker = this;

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
                validated = true;
        } catch(const occ_validation_exception &e) {
                if (e.err == VALIDATION_ERR)
                        action->release_locks();
                action->cleanup();
                validated = false;
        }
        return validated;
}
