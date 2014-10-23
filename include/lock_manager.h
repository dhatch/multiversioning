#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <action.h>
#include <deque>
#include <lock_manager_table.h>
#include <pthread.h>

using namespace std;

class LockManager {    
private:
  LockManagerTable *table;

    bool
    CheckWrite(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    bool
    CheckRead(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    void
    AddTxn(struct TxnQueue *queue, struct EagerRecordInfo *dep);

    void
    RemoveTxn(struct TxnQueue *queue, 
              struct EagerRecordInfo *dep, 
              struct EagerRecordInfo **prev,
              struct EagerRecordInfo **next);

    void
    AdjustRead(struct EagerRecordInfo *dep);

    void
    AdjustWrite(struct EagerRecordInfo *dep);

    bool
    QueueContains(TxnQueue *queue, EagerAction *txn);

    void
    FinishAcquisitions(EagerAction *txn);

    void LockRecord(EagerAction *txn, struct EagerRecordInfo *dep, int cpu);

public:
    //    LockManager(cc_params::TableInit *params, int num_params);
    
    // Acquire and release the mutex protecting a particular hash chain
    virtual void Unlock(EagerAction *txn, int cpu);

    virtual bool Lock(EagerAction *txn, int cpu);
    
    //    virtual void Kill(EagerAction *txn, int cpu);

    //    bool CheckLocks(EagerAction *txn);
};

#endif // LOCK_MANAGER_HH_
