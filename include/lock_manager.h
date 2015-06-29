#ifndef LOCK_MANAGER_HH_
#define LOCK_MANAGER_HH_

#include <lock_manager_table.h>
#include <locking_action.h>
//#include <action.h>
#include <deque>
#include <pthread.h>

using namespace std;

class LockManager {    
 public:
        static uint64_t *tableSizes;

 private:
        LockManagerTable *table;
        
        //        void FinishAcquisitions(locking_action *txn);
        bool LockRecord(locking_action *txn, struct locking_key *dep);  

public:
    LockManager(LockManagerConfig config);
    virtual bool Lock(locking_action *txn);
    virtual void Unlock(locking_action *txn);
    static bool SortCmp(const locking_key &key1, const locking_key &key2);
};

#endif // LOCK_MANAGER_HH_
