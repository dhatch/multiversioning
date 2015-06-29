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
LockManager::CheckRead(struct EagerRecordInfo *dep) {
        //    struct EagerRecordInfo *prev = dep->prev;
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

void LockManager::Unlock(EagerAction *txn, uint32_t cpu)
{
        uint32_t i, num_writes, num_reads;
        for (i = 0; i < num_writes; ++i) 
                table->Unlock(&txn->writeset[i], cpu);
        for (i = 0; i < num_reads; ++i) 
                table->Unlock(&txn->readset[i], cpu);
        txn->finished_execution = true;
}

void
LockManager::FinishAcquisitions(EagerAction *txn) {
        for (size_t i = 0; i < txn->writeset.size(); ++i) 
                table->FinishLock(&txn->writeset[i]);
        for (size_t i = 0; i < txn->readset.size(); ++i) 
                table->FinishLock(&txn->readset[i]);
}

bool LockManager::LockRecord(locking_action *txn, struct EagerRecordInfo *dep,
                             uint32_t cpu)
{
    dep->dependency = txn;
    dep->next = NULL;
    dep->prev = NULL;
    dep->is_held = false;    
    return table->Lock(dep, cpu);
}

bool LockManager::SortCmp(const EagerRecordInfo &key1, 
                          const EagerRecordInfo &key2) {
  if (key1.record.tableId != key2.record.tableId) {
    return key1.record.tableId > key2.record.tableId;
  }
  else {
    return key1.record.key > key2.record.key;
  }
}

bool LockManager::Lock(locking_action *txn)
{
        bool success;
        uint32_t *r_index, *w_index, num_reads, num_writes;
        struct locking_key &write_key, &read_key;

        txn->prepare();
        barrier();
        txn->num_dependencies = 0;
        barrier();

        r_index = &txn->read_index;
        w_index = &txn->write_index;
        num_reads = txn->readset.size();
        num_writes = txn->writeset.size();

        /* Acquire locks in sorted order. */
        while (*r_index < num_reads && *w_index < num_writes) {
                read_key = txn->readset[*read_index];
                write_key = txn->writeset[*write_index];
                
                /* 
                 * A particular key can occur in one of the read- or write-sets,
                 * NOT both!  
                 */
                assert(read_key != write_key);                
                if (read_key < write_key) {
                        *read_index += 1;
                        if (!LockRecord(txn, &txn->readset[*r_index], cpu)) 
                                return false;                        
                } else {
                        *write_index += 1;                        
                        if (!LockRecord(txn, &txn->writeset[*w_index], cpu))
                                return false;
                }
        }
    
        /* At most one of these two loops can be executed. */
        while (*w_index < num_writes) {
                assert(*r_index == num_reads);
                *w_index += 1;
                if (!LockRecord(txn, &txn->writeset[*w_index], cpu))
                        return false;
        }
        while (*r_index < num_reads) {
                assert(*w_index == num_writes);
                *r_index += 1;
                if (!LockRecord(txn, &txn->readset[*r_index], cpu))
                        return false;
        }
        assert(*w_index == num_writes && *r_index = num_reads);
        return true;
}

bool
LockManager::Lock(EagerAction *txn, uint32_t cpu) {
  bool success = true;
  if (txn->sorted == false) {
          std::sort(txn->readset.begin(), txn->readset.end(), SortCmp);
          std::sort(txn->writeset.begin(), txn->writeset.end(), SortCmp);
          txn->sorted = true;
  }
  
  barrier();
    txn->num_dependencies = 0;
    barrier();
    txn->finished_execution = false;

    uint32_t *read_index = &txn->read_index;
    uint32_t *write_index = &txn->write_index;
    
    // Acquire locks in sorted order. Both read and write sets are sorted according to 
    // key we need to merge them together.
    while (*read_index < txn->readset.size() && *write_index < txn->writeset.size()) {
        assert(txn->readset[*read_index] != txn->writeset[*write_index]);
        if (SortCmp(txn->readset[*read_index], txn->writeset[*write_index])) {
            txn->readset[*read_index].is_write = false;
            success = LockRecord(txn, &txn->readset[*read_index], cpu);
            *read_index += 1;
            if (!success)
                    return false;
        }
        else {
            txn->writeset[*write_index].is_write = true;
            success = LockRecord(txn, &txn->writeset[*write_index], cpu);
            *write_index += 1;
            if (!success)
                    return false;
        }
    }
    
    // At most one of these two loops can be executed
    while (*write_index < txn->writeset.size()) {
        assert(*read_index == txn->readset.size());
        txn->writeset[*write_index].is_write = true;
        success = LockRecord(txn, &txn->writeset[*write_index], cpu);
        *write_index += 1;
        if (!success)
                return false;
    }
    while (*read_index < txn->readset.size()) {        
        assert(*write_index == txn->writeset.size());
        txn->readset[*read_index].is_write = false;
        success = LockRecord(txn, &txn->readset[*read_index], cpu);
        *read_index += 1;
        if (!success)
                return false;
    }
    
    assert(*write_index == txn->writeset.size() && 
           *read_index == txn->readset.size());

    //    FinishAcquisitions(txn);
    return true;
}
