#include <lock_manager.h>
#include <algorithm>

uint64_t* LockManager::tableSizes = NULL;

LockManager::LockManager(LockManagerConfig config) {
  table = new LockManagerTable(config);  
}

bool
LockManager::CheckWrite(struct TxnQueue *queue, struct EagerRecordInfo *dep) {
    bool ret = (queue->head == dep);
    assert(!ret || (dep->prev == NULL));
    //    assert(ret);	// XXX: REMOVE ME
    return ret;
}

bool
LockManager::CheckRead(struct TxnQueue *queue, struct EagerRecordInfo *dep) {
    struct EagerRecordInfo *prev = dep->prev;
    if (dep->prev != NULL && (dep->prev->is_write || 
                              !dep->prev->is_held)) {
        return false;
    }
    return true;
}

// Checks if the given queue contains the lock
bool
LockManager::QueueContains(TxnQueue *queue, EagerAction *txn) {
    struct EagerRecordInfo *iter = queue->head;
    while (iter != NULL) {
        if (txn == iter->dependency) {
            return true;
        }
    }
    return false;
}

// Check if the transaction still holds a lock. Used for debugging purposes
/*
bool
LockManager::CheckLocks(EagerAction *txn, int cpu) {
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
      table->GetPtr(
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->writeset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->writeset[i].record.m_key);
        assert(value != NULL);

        struct EagerRecordInfo *dep = &txn->writeset[i];
        lock(&value->lock_word);
        bool check = QueueContains(value, txn);
        unlock(&value->lock_word);        
        if (check) {
            return true;
        }
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {
        Table<uint64_t, TxnQueue> *tbl = 
            m_tables[txn->writeset[i].record.m_table];
        TxnQueue *value = tbl->GetPtr(txn->writeset[i].record.m_key);
        assert(value != NULL);

        struct EagerRecordInfo *dep = &txn->writeset[i];
        lock(&value->lock_word);
        bool check = QueueContains(value, txn);
        unlock(&value->lock_word);        
        if (check) {
            return true;
        }
    }
    return false;
}
*/

void
LockManager::AddTxn(struct TxnQueue *queue, struct EagerRecordInfo *dep) {
    dep->next = NULL;
    dep->prev = NULL;

    if (queue->tail != NULL) {
        //        assert(false);	// XXX: REMOVE ME
        assert(queue->head != NULL);
        dep->prev = queue->tail;
        queue->tail->next = dep;
        queue->tail = dep;
        dep->next = NULL;
    }
    else {
        assert(queue->head == NULL);
        queue->head = dep;
        queue->tail = dep;
        dep->prev = NULL;
        dep->next = NULL;
    }
}

void
LockManager::RemoveTxn(struct TxnQueue *queue, 
                       struct EagerRecordInfo *dep,
                       struct EagerRecordInfo **prev_txn,
                       struct EagerRecordInfo **next_txn) {
    
    struct EagerRecordInfo *prev = dep->prev;
    struct EagerRecordInfo *next = dep->next;

    // If we're at either end of the queue, make appropriate adjustments
    if (prev == NULL) {
        assert(queue->head == dep);
        queue->head = next;
    }
    else {
        assert(prev->next == dep);
        prev->next = next;
    }

    if (next == NULL) {
        assert(queue->tail == dep);
        queue->tail = prev;
    }
    else {
        assert(next->prev == dep);
        next->prev = prev;
    }
    *prev_txn = prev;
    *next_txn = next;
    //    assert(queue->tail == NULL && queue->head == NULL); // XXX: REMOVE ME
}

void
LockManager::AdjustWrite(struct EagerRecordInfo *dep) {
    assert(dep->is_write);
    struct EagerRecordInfo *next = dep->next;
    if (next != NULL) {
        if (next->is_write) {
            next->is_held = true;
            fetch_and_decrement(&next->dependency->num_dependencies);
        }
        else {
            for (struct EagerRecordInfo *iter = next;
                 iter != NULL && !iter->is_write; iter = iter->next) {
                iter->is_held = true;
                fetch_and_decrement(&iter->dependency->num_dependencies);
            }
        }
    }
}

void
LockManager::AdjustRead(struct EagerRecordInfo *dep) {
    assert(!dep->is_write);
    struct EagerRecordInfo *next = dep->next;
    if (next != NULL && dep->prev == NULL && next->is_write) {
        next->is_held = true;
        fetch_and_decrement(&next->dependency->num_dependencies);
    }
}

/*
void
LockManager::RemoveRead(struct TxnQueue *queue, struct EagerRecordInfo *dep) {

    // Splice this element out of the queue
    struct EagerRecordInfo *next = dep->next;
    struct EagerRecordInfo *prev = dep->prev;

    // Try to update the previous txn
    if (prev != NULL) {
        assert(prev->next == dep);
        prev->next = next;
    }
    else {
        assert(queue->head == dep);
        queue->head = next;
    }

    // Try to update the next txn
    if (next != NULL) {
        assert(next->prev == dep);
        next->prev = prev;
    }
    else {
        assert(queue->tail == dep);
        queue->tail = prev;
    }
        
    // If the txn that follows is a write, check if it can proceed
    if (next != NULL && prev == NULL && next->is_write) {
        fetch_and_decrement(&next->dependency->num_dependencies);
    }
}


void
LockManager::Kill(EagerAction *txn, int cpu) {
    struct EagerRecordInfo *prev;
    struct EagerRecordInfo *next;
    
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
        struct EagerRecordInfo *cur = &txn->writeset[i];
        Table<uint64_t, TxnQueue> *tbl = m_tables[cur->record.m_table];
        TxnQueue *value = tbl->GetPtr(cur->record.m_key);
        assert(value != NULL);
        
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        RemoveTxn(value, cur, &prev, &next);

        if (cur->is_held) {
            AdjustWrite(cur);
        }
        else {
            assert(prev != NULL);        
        }
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {
        struct EagerRecordInfo *cur = &txn->readset[i];
        Table<uint64_t, TxnQueue> *tbl = m_tables[cur->record.m_table];
        TxnQueue *value = tbl->GetPtr(cur->record.m_key);
        assert(value != NULL);
        
        lock(&value->lock_word);
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        RemoveTxn(value, cur, &prev, &next);
        if (cur->is_held) {
            AdjustRead(cur);
        }
        else {
            assert(prev != NULL);        
        }
        assert((value->head == NULL && value->tail == NULL) ||
               (value->head != NULL && value->tail != NULL));

        unlock(&value->lock_word);
    }
}
*/

void
LockManager::Unlock(EagerAction *txn, uint32_t cpu) {

  struct EagerRecordInfo *prev;
  struct EagerRecordInfo *next;
    
  for (size_t i = 0; i < txn->writeset.size(); ++i) {
    table->Unlock(&txn->writeset[i], cpu);
    /*
    TxnQueue *value = table->GetPtr(txn->writeset[i].record, cpu);
    assert(value != NULL);

    struct EagerRecordInfo *dep = &txn->writeset[i];

    lock(&value->lock_word);
    // pthread_mutex_lock(&value->mutex);
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));

    assert(dep->is_held);
    RemoveTxn(value, dep, &prev, &next);
    assert(prev == NULL);	// A write better be at the front of the queue
    AdjustWrite(dep);
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));
    unlock(&value->lock_word);
    */
    // pthread_mutex_unlock(&value->mutex);
  }
  for (size_t i = 0; i < txn->readset.size(); ++i) {
    table->Unlock(&txn->readset[i], cpu);
    /*
    TxnQueue *value = table->GetPtr(txn->readset[i].record, cpu);
    assert(value != NULL);

    struct EagerRecordInfo *dep = &txn->readset[i];
    lock(&value->lock_word);
    // pthread_mutex_lock(&value->mutex);

    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));

    assert(dep->is_held);
    RemoveTxn(value, dep, &prev, &next);
    AdjustRead(dep);
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));
    unlock(&value->lock_word);
    // pthread_mutex_unlock(&value->mutex);
    */
  }
  //  FinishAcquisitions(txn);
  txn->finished_execution = true;
}

void
LockManager::FinishAcquisitions(EagerAction *txn) {
    for (size_t i = 0; i < txn->writeset.size(); ++i) {
      table->FinishLock(&txn->writeset[i]);
      //        unlock(txn->writeset[i].latch);
        //        pthread_mutex_unlock(txn->writeset[i].latch);
    }
    for (size_t i = 0; i < txn->readset.size(); ++i) {
      table->FinishLock(&txn->readset[i]);
      //unlock(txn->readset[i].latch);
        // pthread_mutex_unlock(txn->readset[i].latch);
    }
}

bool
LockManager::LockRecord(EagerAction *txn, struct EagerRecordInfo *dep, 
                        uint32_t cpu) {
    dep->dependency = txn;
    dep->next = NULL;
    dep->prev = NULL;
    dep->is_held = false;
    
    return table->Lock(dep, cpu);
    /*
    TxnQueue *value = table->GetPtr(dep->record, cpu);
    assert(value != NULL);
    // dep->latch = &value->mutex;
    dep->latch = &value->lock_word;

    // Atomically add the transaction to the lock queue and check whether 
    // it managed to successfully acquire the lock
    lock(&value->lock_word);
    //    pthread_mutex_lock(&value->mutex);
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));

    AddTxn(value, dep);

    if (dep->is_write) {
        if ((dep->is_held = CheckWrite(value, dep)) == false) {
            fetch_and_increment(&txn->num_dependencies);
        }
        else {
            assert(dep->prev == NULL);
        }
    }
    else {
        if ((dep->is_held = CheckRead(value, dep)) == false) {
            fetch_and_increment(&txn->num_dependencies);
        }
    }
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));    
    */
}

/*
void
LockManager::AcquireRead(EagerAction *txn, struct EagerRecordInfo *dep) {
    dep->dependency = txn;
    dep->is_write = false;
    dep->is_held = false;

    // We *must* find the key-value pair
    Table<uint64_t, TxnQueue> *tbl = m_tables[dep->record.m_table];
    TxnQueue *value = tbl->GetPtr(dep->record.m_key);
    assert(value != NULL);
    dep->latch = &value->mutex;

    // Atomically add the transaction to the lock queue and check whether 
    // it managed to successfully acquire the lock
    //        lock(&value->lock_word);
    pthread_mutex_lock(&value->mutex);
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));

    AddTxn(value, dep);
    if ((dep->is_held = CheckRead(value, dep)) == false) {
        fetch_and_increment(&txn->num_dependencies);
    }        
    assert((value->head == NULL && value->tail == NULL) ||
           (value->head != NULL && value->tail != NULL));    
}
*/




bool LockManager::SortCmp(const EagerRecordInfo &key1, 
                          const EagerRecordInfo &key2) {
  if (key1.record.tableId != key2.record.tableId) {
    return key1.record.tableId > key2.record.tableId;
  }
  else {
    return key1.record.hash > key2.record.hash;
  }
}

bool
LockManager::Lock(EagerAction *txn, uint32_t cpu) {
  bool success = true;
  barrier();
    txn->num_dependencies = 0;
    barrier();
    txn->finished_execution = false;
    size_t read_index = 0;
    size_t write_index = 0;

    std::sort(txn->readset.begin(), txn->readset.end(), SortCmp);
    std::sort(txn->writeset.begin(), txn->writeset.end(), SortCmp);
    
    // Acquire locks in sorted order. Both read and write sets are sorted according to 
    // key we need to merge them together.
    while (read_index < txn->readset.size() && write_index < txn->writeset.size()) {
        assert(txn->readset[read_index] != txn->writeset[write_index]);
        if (SortCmp(txn->readset[read_index], txn->writeset[write_index])) {
            txn->readset[read_index].is_write = false;
            success &= LockRecord(txn, &txn->readset[read_index], cpu);
            read_index += 1;
        }
        else {
            txn->writeset[write_index].is_write = true;
            success &= LockRecord(txn, &txn->writeset[write_index], cpu);
            write_index += 1;
        }
    }
    
    // At most one of these two loops can be executed
    while (write_index < txn->writeset.size()) {
        assert(read_index == txn->readset.size());
        txn->writeset[write_index].is_write = true;
        success &= LockRecord(txn, &txn->writeset[write_index], cpu);
        write_index += 1;
    }
    while (read_index < txn->readset.size()) {        
        assert(write_index == txn->writeset.size());
        txn->readset[read_index].is_write = false;
        success &= LockRecord(txn, &txn->readset[read_index], cpu);
        read_index += 1;
    }
    
    assert(write_index == txn->writeset.size() && 
           read_index == txn->readset.size());

    FinishAcquisitions(txn);
    return success;
}
