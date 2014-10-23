#include <eager_worker.h>

EagerWorker::EagerWorker(EagerWorkerConfig config) 
    : Runnable(config.cpu) {
  this->config = config;
    m_queue_head = NULL;
    m_queue_tail = NULL;    
    m_num_elems = 0;
    m_num_done = 0;
}

void EagerWorker::Init() {
}

void
EagerWorker::StartWorking() {
    WorkerFunction();
}

void
EagerWorker::Enqueue(EagerAction *txn) {
    if (m_queue_head == NULL) {
        assert(m_queue_tail == NULL);
        m_queue_head = txn;
        m_queue_tail = txn;
        txn->prev = NULL;
        txn->next = NULL;
    }
    else {
        m_queue_tail->next = txn;
        txn->next = NULL;
        txn->prev = m_queue_tail;
        m_queue_tail = txn;
    }
    m_num_elems += 1;
    assert((m_queue_head == NULL && m_queue_tail == NULL) ||
           (m_queue_head != NULL && m_queue_tail != NULL));
}

void
EagerWorker::RemoveQueue(EagerAction *txn) {
    EagerAction *prev = txn->prev;
    EagerAction *next = txn->next;

    if (m_queue_head == txn) {
        assert(txn->prev == NULL);
        m_queue_head = txn->next;
    }
    else {
        prev->next = next;
    }
    
    if (m_queue_tail == txn) {
        assert(txn->next == NULL);
        m_queue_tail = txn->prev;
    }
    else {
        next->prev = prev;
    }
    
    m_num_elems -= 1;
    assert(m_num_elems >= 0);
    assert((m_queue_head == NULL && m_queue_tail == NULL) ||
           (m_queue_head != NULL && m_queue_tail != NULL));
}

uint32_t
EagerWorker::QueueCount(EagerAction *iter) {
    if (iter == NULL) {
        return 0;
    }
    else {
        return 1+QueueCount(iter->next);
    }
}

void
EagerWorker::CheckReady() {
  for (EagerAction *iter = m_queue_head; iter != NULL; iter = iter->next) {
    if (iter->num_dependencies == 0) {
      RemoveQueue(iter);
      DoExec(iter);
    }
  }
}

void
EagerWorker::TryExec(EagerAction *txn) {
  if (config.mgr->Lock(txn, config.cpu)) {
        assert(txn->num_dependencies == 0);
        txn->Run();
        config.mgr->Unlock(txn, config.cpu);

        assert(txn->finished_execution);
        /*
        EagerAction *link;
        if (txn->IsLinked(&link)) {
            TryExec(link);
        }
        else {
            m_num_done += 1;
            clock_gettime(CLOCK_REALTIME, &txn->end_time);
            //            txn->end_rdtsc_time = rdtsc();
            m_output_queue->EnqueueBlocking((uint64_t)txn);
        }
        */
    }    
    else {
        m_num_done += 1;
        Enqueue(txn);
    }
}

void
EagerWorker::DoExec(EagerAction *txn) {
    assert(txn->num_dependencies == 0);
    txn->Run();
    config.mgr->Unlock(txn, config.cpu);
    //    txn->PostExec();
    /*
    EagerAction *link;
    if (txn->IsLinked(&link)) {
        TryExec(link);
    }
    else {
        m_num_done += 1;
        clock_gettime(CLOCK_REALTIME, &txn->end_time);
        //        txn->end_rdtsc_time = rdtsc();
        m_output_queue->EnqueueBlocking((uint64_t)txn);
    }
    */
}

void
EagerWorker::WorkerFunction() {
  EagerAction *txn;

  // Each iteration of this loop executes a batch of transactions
  while (true) {
    EagerActionBatch batch = config.inputQueue->DequeueBlocking();
      
    for (uint32_t i = 0; i < batch.batchSize; ++i) {
      // Ensure we haven't exceeded threshold of max deferred txns. If we have, 
      // exec pending txns so we get below the threshold.
      if (m_num_elems < config.maxPending) {
        TryExec(txn);
      }
      else {
        while (m_num_elems >= config.maxPending) {
          CheckReady();
        }
      }
    }

    // Finish deferred txns
    while (m_num_elems != 0) {
      CheckReady();
    }
    
    // Signal that this batch is done
    config.outputQueue->EnqueueBlocking(batch);
  }
}
