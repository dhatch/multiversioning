#include <eager_worker.h>

locking_worker::locking_worker(locking_worker_config config,
                               RecordBuffersConfig rb_conf) 
    : Runnable(config.cpu)
{
        this->config = config;
        m_queue_head = NULL;
        m_queue_tail = NULL;    
        m_num_elems = 0;
        m_num_done = 0;
        this->bufs = new(config.cpu) RecordBuffers(rb_conf);
}

void locking_worker::Init()
{
}

void
locking_worker::StartWorking()
{
        WorkerFunction();
}

void locking_worker::Enqueue(locking_action *txn)
{
        if (m_queue_head == NULL) {
                assert(m_queue_tail == NULL);
                m_queue_head = txn;
                m_queue_tail = txn;
                txn->prev = NULL;
                txn->next = NULL;
        }else {
                m_queue_tail->next = txn;
                txn->next = NULL;
                txn->prev = m_queue_tail;
                m_queue_tail = txn;
        }
        m_num_elems += 1;
        assert((m_queue_head == NULL && m_queue_tail == NULL) ||
               (m_queue_head != NULL && m_queue_tail != NULL));
}

void locking_worker::RemoveQueue(locking_action *txn)
{
        locking_action *prev = txn->prev;
        locking_action *next = txn->next;

        if (m_queue_head == txn) {
                assert(txn->prev == NULL);
                m_queue_head = txn->next;
        } else {
                prev->next = next;
        }
    
        if (m_queue_tail == txn) {
                assert(txn->next == NULL);
                m_queue_tail = txn->prev;
        } else {
                next->prev = prev;
        }
    
        m_num_elems -= 1;
        assert(m_num_elems >= 0);
        assert((m_queue_head == NULL && m_queue_tail == NULL) ||
               (m_queue_head != NULL && m_queue_tail != NULL));
}

uint32_t
locking_worker::QueueCount(locking_action *iter)
{
        if (iter == NULL) 
                return 0;
        else
                return 1+QueueCount(iter->next);
}

void locking_worker::CheckReady()
{
        locking_action *iter;
        for (iter = m_queue_head; iter != NULL; iter = iter->next) {
                if (iter->num_dependencies == 0 && config.mgr->Lock(iter)) {
                                RemoveQueue(iter);
                                DoExec(iter);
                }
        }
}

void locking_worker::TryExec(locking_action *txn)
{
        txn->tables = this->config.tables;
        if (config.mgr->Lock(txn)) {
                assert(txn->num_dependencies == 0);
                assert(txn->bufs == NULL);
                txn->bufs = this->bufs;
                txn->worker = this;
                txn->Run();
                config.mgr->Unlock(txn);                
                assert(txn->finished_execution);
        } else {
                m_num_done += 1;
                Enqueue(txn);
        }
}

void locking_worker::DoExec(locking_action *txn)
{
        assert(txn->num_dependencies == 0);
        assert(txn->bufs == NULL);
        txn->bufs = this->bufs;    
        txn->worker = this;
        txn->Run();
        config.mgr->Unlock(txn);
}

void
locking_worker::WorkerFunction()
{
        locking_action_batch batch;
        //        double results[1000];

        // Each iteration of this loop executes a batch of transactions
        while (true) {
                batch = config.inputQueue->DequeueBlocking();
                for (uint32_t i = 0; i < batch.batchSize; ++i) {
                        
                        // Ensure we haven't exceeded threshold of max deferred
                        // txns. If we have, exec pending txns so we get below
                        // the threshold.
                        if ((uint32_t)m_num_elems < config.maxPending) 
                                TryExec(batch.batch[i]);
                        else 
                                while (m_num_elems >= config.maxPending) 
                                        CheckReady();
                }
                
                // Finish deferred txns
                while (m_num_elems != 0) 
                        CheckReady();
        
                // Signal that this batch is done
                config.outputQueue->EnqueueBlocking(batch);
        }
 
}
