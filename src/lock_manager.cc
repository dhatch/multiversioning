#include <lock_manager.h>
#include <algorithm>
#include <lock_manager_table.h>

LockManager::LockManager(LockManagerConfig config)
{
        uint32_t i;
        table = new LockManagerTable(config);
        tableSizes = (uint64_t*)malloc(sizeof(uint64_t)*config.numTables);
        for (i = 0; i < config.numTables; ++i) 
                tableSizes[i] = (uint64_t)config.tableSizes[i];
}

bool LockManager::LockRecord(locking_action *txn, struct locking_key *k)
{
        assert(k->dependency == txn && k->is_held == false);
        k->next = NULL;
        k->prev = NULL;
        return table->Lock(k);
}

bool LockManager::SortCmp(const locking_key &key1, const locking_key &key2)
{
        return key1 < key2;
}

bool LockManager::Lock(locking_action *txn)
{
        uint32_t *r_index, *w_index, num_reads, num_writes;
        struct locking_key write_key, read_key;
        bool acquired = true;
        
        txn->prepare();
        
        r_index = &txn->read_index;
        w_index = &txn->write_index;
        num_reads = txn->readset.size();
        num_writes = txn->writeset.size();

        /* Acquire locks in sorted order. */
        while (acquired && *r_index < num_reads && *w_index < num_writes) {
                read_key = txn->readset[*r_index];
                write_key = txn->writeset[*w_index];
                
                /* 
                 * A particular key can occur in one of the read- or write-sets,
                 * NOT both!  
                 */
                assert(read_key != write_key);                
                if (read_key < write_key) {
                        acquired = LockRecord(txn, &txn->readset[*r_index]);
                        *r_index += 1;
                } else {
                        acquired = LockRecord(txn, &txn->writeset[*w_index]);
                        *w_index += 1;
                }
        }
    
        /* At most one of these two loops can be executed. */
        while (acquired && *w_index < num_writes) {
                assert(*r_index == num_reads);
                acquired = LockRecord(txn, &txn->writeset[*w_index]);
                *w_index += 1;
        }
        while (acquired && *r_index < num_reads) {
                assert(*w_index == num_writes);
                acquired = LockRecord(txn, &txn->readset[*r_index]);
                *r_index += 1;
        }
        assert(!acquired || (*w_index == num_writes && *r_index == num_reads));
        return acquired;
}

void LockManager::Unlock(locking_action *txn)
{
        uint32_t i, num_writes, num_reads;
        
        num_writes = txn->writeset.size();
        num_reads = txn->readset.size();
        for (i = 0; i < num_writes; ++i) 
                table->Unlock(&txn->writeset[i]);
        for (i = 0; i < num_reads; ++i) 
                table->Unlock(&txn->readset[i]);
        txn->finished_execution = true;
}

