#ifndef         SMALL_BANK_H_
#define         SMALL_BANK_H_

#include <vector>
#include <cstdlib>

#include <action.h>

enum SmallBankTable {
  CHECKING = 0,
  SAVINGS,
};

struct SmallBankRecord {
  long amount;
  //  char timestamp[248];
};

namespace OCCSmallBank {
        class Balance : public OCCAction {
        private:
                long totalBalance;
                
        public:
                Balance(uint64_t customerId);
                virtual bool Run();
        };
        
        class DepositChecking : public OCCAction {
        private:
                long amount;
        
        public:
                DepositChecking(uint64_t customer, long amount);
                virtual bool Run();
        };

        class TransactSaving : public OCCAction {    
        private:
                long amount;
        public:
                TransactSaving(uint64_t customer, long amount);
                virtual bool Run();
        };

        class Amalgamate : public OCCAction {
        public:
                Amalgamate(uint64_t fromCustomer, uint64_t toCustomer);
                virtual bool Run();
        };
  
        class WriteCheck : public OCCAction {
        private:
                long amount;
    
        public:
                WriteCheck(uint64_t customer, long amount);
                virtual bool Run();
        };  
};

namespace LockingSmallBank {
  class Balance : public EagerAction {
  private:
    long totalBalance;
    //    char timestamp[248];
  
  public:
    Balance(uint64_t customerId, uint64_t numAccounts, char *time);

    virtual bool Run();
  };

  class DepositChecking : public EagerAction {
  private:
    long amount;
    //    char timestamp[248];

  public:
    DepositChecking(uint64_t customer, long amount, uint64_t numAccounts, 
                    char *time);
    virtual bool Run();
  };

  class TransactSaving : public EagerAction {    
  private:
    long amount;
    //    char timestamp[248];

  public:
    TransactSaving(uint64_t customer, long amount, uint64_t numAccounts, 
                   char *time);
    virtual bool Run();
  };

  class Amalgamate : public EagerAction {
  private:
    //    char timestamp[248];

  public:
    Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, 
               uint64_t numAccounts,
               char *time);
    virtual bool Run();
  };
  
  class WriteCheck : public EagerAction {
  private:
    long amount;
    //    char timestamp[248];
    
  public:
    WriteCheck(uint64_t customer, long amount, uint64_t numAccounts, 
               char *time);
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
    //    char timestamp[248];
    
  public:
      Balance(uint64_t customerId, char *time);
    
    virtual bool Run();
  };

  class DepositChecking : public Action {
  private:
    long amount;
    //    char timestamp[248];
    
  public:
    DepositChecking(uint64_t customerId, long amount, char *time);

    virtual bool Run();
  };

  class TransactSaving : public Action {
  private:
    long amount;
    //    char timestamp[248];
    
  public:
    TransactSaving(uint64_t customerId, long amount, char *time);

    virtual bool Run();
  };
  
  class Amalgamate : public Action {
  private:
    //    char timestamp[248];
  public:
    Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, char *time);

    virtual bool Run();
  };
  
  class WriteCheck : public Action {
  private:
    long amount;
    //    char timestamp[248];
     
  public:
    WriteCheck(uint64_t customer, long amount, char *time);

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
