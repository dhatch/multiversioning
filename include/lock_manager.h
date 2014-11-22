#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <lock_manager_table.h>
#include <action.h>
#include <deque>
#include <pthread.h>

using namespace std;

class LockManager {    
 public:
  static uint64_t *tableSizes;

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

    bool LockRecord(EagerAction *txn, struct EagerRecordInfo *dep, uint32_t cpu);

public:
    LockManager(LockManagerConfig config);
    
    // Acquire and release the mutex protecting a particular hash chain
    virtual void Unlock(EagerAction *txn, uint32_t cpu);


    virtual bool Lock(EagerAction *txn, uint32_t cpu);


    static bool SortCmp(const EagerRecordInfo &key1, const EagerRecordInfo &key2);    
    //    virtual void Kill(EagerAction *txn, int cpu);

    //    bool CheckLocks(EagerAction *txn);
};

#endif // LOCK_MANAGER_HH_
