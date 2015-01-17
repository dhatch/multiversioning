
#include <small_bank.h>

LockingSmallBank::Balance::Balance(uint64_t customer, uint64_t numAccounts, 
                                   char *time) {
  this->totalBalance = 0;
  // memcpy(this->timestamp, time, 248);
  AddReadKey(CHECKING, customer, numAccounts);
  AddReadKey(SAVINGS, customer, numAccounts);
}

bool LockingSmallBank::Balance::Run() {
  SmallBankRecord *checkingBalance = (SmallBankRecord*)ReadRef(0);
  SmallBankRecord *savingsBalance = (SmallBankRecord*)ReadRef(1);
  this->totalBalance = checkingBalance->amount + savingsBalance->amount;
  return true;
}

LockingSmallBank::DepositChecking::DepositChecking(uint64_t customer,
                                                   long amount,
                                                   uint64_t numAccounts,
                                                   char *time) {
  // memcpy(this->timestamp, time, 248);
  this->amount = amount;
  AddWriteKey(CHECKING, customer,numAccounts);
}

bool LockingSmallBank::DepositChecking::Run() {
  SmallBankRecord *checkingBalance = (SmallBankRecord*)(WriteRef(0));
  checkingBalance->amount += this->amount;
  //  // memcpy(checkingBalance->timestamp, this->timestamp, 248);
  return true;
}

LockingSmallBank::TransactSaving::TransactSaving(uint64_t customer,
                                                 long amount,
                                                 uint64_t numAccounts,
                                                 char *time) {
  this->amount = 0;
  // memcpy(this->timestamp, time, 248);
  AddWriteKey(SAVINGS, customer, numAccounts);
}

bool LockingSmallBank::TransactSaving::Run() {
  SmallBankRecord *savings = (SmallBankRecord*)(WriteRef(0));
  savings->amount += this->amount;
  //  // memcpy(savings->timestamp, this->timestamp, 248);
}

LockingSmallBank::Amalgamate::Amalgamate(uint64_t fromCustomer,
                                         uint64_t toCustomer,
                                         uint64_t numAccounts,
                                         char *time) {
  // // memcpy(this->timestamp, time, 248);
  AddWriteKey(CHECKING, fromCustomer, numAccounts);
  AddWriteKey(SAVINGS, fromCustomer, numAccounts);
  AddWriteKey(CHECKING, toCustomer, numAccounts);
}

bool LockingSmallBank::Amalgamate::Run() {
  long sum = 0;
  sum += ((SmallBankRecord*)WriteRef(0))->amount;
  sum += ((SmallBankRecord*)WriteRef(1))->amount;
  long oldChecking = ((SmallBankRecord*)WriteRef(2))->amount;

  // Zero "fromCustomer's" balance.
  ((SmallBankRecord*)WriteRef(0))->amount = 0;
  ((SmallBankRecord*)WriteRef(1))->amount = 0;

  // Update "toCustomer's" balance.
  ((SmallBankRecord*)WriteRef(2))->amount = sum + oldChecking;  

  // // memcpy(((SmallBankRecord*)WriteRef(0))->timestamp, this->timestamp, 248);
  // // memcpy(((SmallBankRecord*)WriteRef(1))->timestamp, this->timestamp, 248);
  // // memcpy(((SmallBankRecord*)WriteRef(2))->timestamp, this->timestamp, 248);
  return true;
}

LockingSmallBank::WriteCheck::WriteCheck(uint64_t customer, long amount,
                                         uint64_t numAccounts,
                                         char *time) {
  this->amount = amount;
  // // memcpy(this->timestamp, time, 248);
  AddReadKey(SAVINGS, customer, numAccounts);
  AddWriteKey(CHECKING, customer, numAccounts);
}

bool LockingSmallBank::WriteCheck::Run() {
  long sum = 0;
  sum += ((SmallBankRecord*)WriteRef(0))->amount;
  sum += ((SmallBankRecord*)ReadRef(0))->amount;
  sum -= amount;
 
  if (sum < 0) {
    amount += 1;
  }
  ((SmallBankRecord*)WriteRef(0))->amount -= amount;
  // // memcpy(((SmallBankRecord*)WriteRef(0))->timestamp, this->timestamp, 248);
  return true;
}

MVSmallBank::LoadCustomerRange::LoadCustomerRange(uint64_t start, uint64_t end)
  : Action() {
  assert(end >= start);
  this->numCustomers = (end - start + 1);

  for (uint64_t i = start; i <= end; ++i) {
    long savings = rand() % 100;
    long checking = rand() % 100;
    balances.push_back(savings);
    balances.push_back(checking);

    AddWriteKey(SAVINGS, i);
    AddWriteKey(CHECKING, i);
  }
}

bool MVSmallBank::LoadCustomerRange::Run() {

  for (uint64_t i = 0; i < this->numCustomers; ++i) {
    long savings = balances[2*i];
    long checking = balances[2*i+1];
    ((SmallBankRecord*)GetWriteRef(2*i))->amount = savings;
    ((SmallBankRecord*)GetWriteRef(2*i+1))->amount = checking;
  }
  
  return true;
}

MVSmallBank::Balance::Balance(uint64_t customer, char *time) : Action() {
  // // memcpy(this->timestamp, time, 248);
  this->totalBalance = 0;
  AddReadKey(CHECKING, customer);
  AddReadKey(SAVINGS, customer);
}

// Read the customer's checking and savings balance, and add their sum to
// "totalBalance"
bool MVSmallBank::Balance::Run() {
  //  assert(false);
  assert(readset.size() == 2);
  assert(writeset.size() == 0);
  SmallBankRecord *checkingBalance = (SmallBankRecord*)Read(0); 
  SmallBankRecord *savingsBalance = (SmallBankRecord*)Read(1);
  this->totalBalance = checkingBalance->amount + savingsBalance->amount;
  return true;
}

MVSmallBank::DepositChecking::DepositChecking(uint64_t customer, long amount, char *time)
  : Action() {
  // // memcpy(this->timestamp, time, 248);
  this->amount = amount;
  AddReadKey(CHECKING, customer);
  AddWriteKey(CHECKING, customer);
}

// Deposit "amount" into the customer's checking account.
bool MVSmallBank::DepositChecking::Run() {
  //  assert(false);
  assert(readset.size() == 1);
  assert(writeset.size() == 1);
  SmallBankRecord *oldCheckingBalance = (SmallBankRecord*)Read(0);
  SmallBankRecord *newCheckingBalance = (SmallBankRecord*)GetWriteRef(0);
  newCheckingBalance->amount = oldCheckingBalance->amount + this->amount;
  // // memcpy(newCheckingBalance->timestamp, this->timestamp, 248);
  return true;
}

MVSmallBank::TransactSaving::TransactSaving(uint64_t customer, long amount, 
                                            char *time)
  : Action() {
  // // memcpy(this->timestamp, time, 248);
  this->amount = 0;
  AddReadKey(SAVINGS, customer);
  AddWriteKey(SAVINGS, customer);
}

bool MVSmallBank::TransactSaving::Run() {
  //  assert(false);
  assert(readset.size() == 1);
  assert(writeset.size() == 1);
  SmallBankRecord *oldSavings = (SmallBankRecord*)Read(0);
  SmallBankRecord *savings = (SmallBankRecord*)GetWriteRef(0);
  savings->amount = oldSavings->amount + this->amount;
  // // memcpy(savings->timestamp, this->timestamp, 248);
  return true;
}

MVSmallBank::Amalgamate::Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, 
                                    char *time)
  : Action() {
  // // memcpy(this->timestamp, time, 248);
  AddReadKey(CHECKING, fromCustomer);
  AddReadKey(SAVINGS, fromCustomer);
  AddReadKey(CHECKING, toCustomer);

  AddWriteKey(CHECKING, fromCustomer);
  AddWriteKey(SAVINGS, fromCustomer);
  AddWriteKey(CHECKING, toCustomer);
}

bool MVSmallBank::Amalgamate::Run() {
  //  assert(false);
  assert(readset.size() == 3);
  assert(writeset.size() == 3);

  long sum = 0;
  sum += ((SmallBankRecord*)Read(0))->amount;
  sum += ((SmallBankRecord*)Read(1))->amount;
  long oldChecking = ((SmallBankRecord*)Read(2))->amount;

  // Zero "fromCustomer's" balance.
  ((SmallBankRecord*)GetWriteRef(0))->amount = 0;
  ((SmallBankRecord*)GetWriteRef(1))->amount = 0;

  // Update "toCustomer's" balance.
  ((SmallBankRecord*)GetWriteRef(2))->amount = sum + oldChecking;  
  
  // // memcpy(((SmallBankRecord*)GetWriteRef(0))->timestamp, this->timestamp, 248);
  // // memcpy(((SmallBankRecord*)GetWriteRef(1))->timestamp, this->timestamp, 248);
  // // memcpy(((SmallBankRecord*)GetWriteRef(2))->timestamp, this->timestamp, 248);
  return true;
}

MVSmallBank::WriteCheck::WriteCheck(uint64_t customer, long amount, 
                                    char *time) {
  // // memcpy(this->timestamp, time, 248);
  this->amount = amount;
  AddReadKey(CHECKING, customer);
  AddReadKey(SAVINGS, customer);
  AddWriteKey(CHECKING, customer);
}

bool MVSmallBank::WriteCheck::Run() {
  //  assert(false);
  assert(readset.size() == 2);
  assert(writeset.size() == 1);
  long sum = 0;
  sum += ((SmallBankRecord*)Read(0))->amount;
  sum += ((SmallBankRecord*)Read(1))->amount;
  sum -= amount;
 
  if (sum < 0) {
    amount += 1;
  }

  // // memcpy(((SmallBankRecord*)GetWriteRef(0))->timestamp, this->timestamp, 248);
  ((SmallBankRecord*)GetWriteRef(0))->amount -= amount;
  return true;
}
