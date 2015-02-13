#ifndef         SMALL_BANK_H_
#define         SMALL_BANK_H_

#include <vector>
#include <cstdlib>

#include <mv_action.h>
#include <occ_action.h>
#include <action.h>

#define METADATA_SIZE 256

enum SmallBankTable {
  CHECKING = 0,
  SAVINGS,
};

struct SmallBankRecord {
        long amount;
        char meta_data[METADATA_SIZE];
};

namespace OCCSmallBank {
        class Balance : public OCCAction {
        private:
                long totalBalance;
                char *meta_data;
                
        public:
                Balance(uint64_t customerId, char *meta_data);
                virtual occ_txn_status Run();
        };
        
        class DepositChecking : public OCCAction {
        private:
                long amount;
                char *meta_data;
        
        public:
                DepositChecking(uint64_t customer, long amount,
                                char *meta_data);
                virtual occ_txn_status Run();
        };

        class TransactSaving : public OCCAction {    
        private:
                long amount;
                char *meta_data;
        public:
                TransactSaving(uint64_t customer, long amount, char *meta_data);
                virtual occ_txn_status Run();
        };

        class Amalgamate : public OCCAction {
                char *meta_data;
        public:
                Amalgamate(uint64_t fromCustomer, uint64_t toCustomer,
                           char *meta_data);
                virtual occ_txn_status Run();
        };
  
        class WriteCheck : public OCCAction {
        private:
                long amount;
                char *meta_data;
        public:
                WriteCheck(uint64_t customer, long amount, char *meta_data);
                virtual occ_txn_status Run();
        };  
};

namespace LockingSmallBank {
  class Balance : public EagerAction {
  private:
    long totalBalance;
    char *meta_data;
  
  public:
    Balance(uint64_t customerId, uint64_t numAccounts, char *meta);

    virtual bool Run();
  };

  class DepositChecking : public EagerAction {
  private:
    long amount;
    char *meta_data;

  public:
    DepositChecking(uint64_t customer, long amount, uint64_t numAccounts, 
                    char *meta);
    virtual bool Run();
  };

  class TransactSaving : public EagerAction {    
  private:
    long amount;
    char *meta_data;

  public:
    TransactSaving(uint64_t customer, long amount, uint64_t numAccounts, 
                   char *meta);
    virtual bool Run();
  };

  class Amalgamate : public EagerAction {
  private:
          char *meta_data;

  public:
    Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, 
               uint64_t numAccounts,
               char *meta);
    virtual bool Run();
  };
  
  class WriteCheck : public EagerAction {
  private:
    long amount;
    char *meta_data;
    
  public:
    WriteCheck(uint64_t customer, long amount, uint64_t numAccounts, 
               char *meta);
    virtual bool Run();
  };  
};

namespace MVSmallBank {

  class LoadCustomerRange : public Action {
  private:
    std::vector<long> balances;
    uint32_t numCustomers;
    
  public:
    LoadCustomerRange(uint64_t start, uint64_t end);

    virtual bool Run();
  };
  
  class Balance : public Action {
  private:
    long totalBalance;
    char *meta_data;
    
  public:
      Balance(uint64_t customerId, char *meta_data);
    
    virtual bool Run();
  };

  class DepositChecking : public Action {
  private:
    long amount;
    char *meta_data;
    
  public:
    DepositChecking(uint64_t customerId, long amount, char *meta_data);

    virtual bool Run();
  };

  class TransactSaving : public Action {
  private:
    long amount;
    char *meta_data;
    
  public:
    TransactSaving(uint64_t customerId, long amount, char *meta_data);

    virtual bool Run();
  };
  
  class Amalgamate : public Action {
  private:
          char *meta_data;
  public:
    Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, char *meta_data);

    virtual bool Run();
  };
  
  class WriteCheck : public Action {
  private:
    long amount;
    char *meta_data;
     
  public:
    WriteCheck(uint64_t customer, long amount, char *meta_data);

    virtual bool Run();
  };
};
class MVBalance : public Action {

 public:
  virtual bool Run();
  
};



class LockingBalance {

};



#endif          // SMALL_BANK_H_
