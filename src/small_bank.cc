#include <small_bank.h>

OCCSmallBank::Balance::Balance(uint64_t customer, char *meta_data)
{
        this->totalBalance = 0;
        this->meta_data = meta_data;
        AddReadKey(CHECKING, customer, false);
        AddReadKey(SAVINGS, customer, false);
}

occ_txn_status OCCSmallBank::Balance::Run()
{
        occ_txn_status status;
        SmallBankRecord *checking = (SmallBankRecord*)readset[0].GetValue();
        SmallBankRecord *savings = (SmallBankRecord*)readset[1].GetValue();
        uint32_t num_ints = METADATA_SIZE/4;
        uint32_t *int_meta_data = (uint32_t*)meta_data;
        uint32_t *checking_meta = (uint32_t*)checking->meta_data;
        uint32_t *savings_meta = (uint32_t*)savings->meta_data;
        for (uint32_t i = 0; i < num_ints; ++i) {
                int_meta_data[i] = checking_meta[i];
                int_meta_data[i] += savings_meta[i];
        }
        this->totalBalance = checking->amount + savings->amount;
        status.validation_pass = true;
        status.commit = true;
        return status;
}

OCCSmallBank::DepositChecking::DepositChecking(uint64_t customer, long amount,
                                               char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        AddReadKey(CHECKING, customer, true);
        AddWriteKey(CHECKING, customer);
}

occ_txn_status OCCSmallBank::DepositChecking::Run()
{
        occ_txn_status status;
        SmallBankRecord *checkingBalance =
                (SmallBankRecord*)readset[0].GetValue();
        long oldBalance = checkingBalance->amount;
        SmallBankRecord *newBalance = (SmallBankRecord*)writeset[0].GetValue();
        newBalance->amount += this->amount;
        memcpy(newBalance->meta_data, meta_data, METADATA_SIZE);
        status.validation_pass = true;
        status.commit = true;
        return status;
}


OCCSmallBank::TransactSaving::TransactSaving(uint64_t customer, long amount,
                                             char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        AddReadKey(SAVINGS, customer, true);
        AddWriteKey(SAVINGS, customer);
}

occ_txn_status OCCSmallBank::TransactSaving::Run()
{
        occ_txn_status status;
        SmallBankRecord *read = (SmallBankRecord*)readset[0].GetValue();
        SmallBankRecord *write  = (SmallBankRecord*)writeset[0].GetValue();
        write->amount = read->amount + this->amount;
        memcpy(write->meta_data, meta_data, METADATA_SIZE);
        status.validation_pass = true;
        status.commit = true;
        return status;

}

OCCSmallBank::Amalgamate::Amalgamate(uint64_t fromCustomer, uint64_t toCustomer,
                                     char *meta_data)
{
        this->meta_data = meta_data;
        AddReadKey(CHECKING, fromCustomer, true);
        AddReadKey(SAVINGS, fromCustomer, true);
        AddReadKey(CHECKING, toCustomer, true);
        AddWriteKey(CHECKING, fromCustomer);
        AddWriteKey(SAVINGS, fromCustomer);
        AddWriteKey(CHECKING, toCustomer);
}

occ_txn_status OCCSmallBank::Amalgamate::Run()
{
        occ_txn_status status;
        long sum = 0;
        SmallBankRecord *fromChecking, *fromSavings, *toChecking;
        sum += ((SmallBankRecord*)readset[0].GetValue())->amount;
        sum += ((SmallBankRecord*)readset[1].GetValue())->amount;
        sum += ((SmallBankRecord*)readset[2].GetValue())->amount;
        fromChecking = (SmallBankRecord*)writeset[0].GetValue();
        fromSavings = (SmallBankRecord*)writeset[1].GetValue();
        toChecking = (SmallBankRecord*)writeset[2].GetValue();
        fromChecking->amount = 0;
        memcpy(fromChecking->meta_data, meta_data, METADATA_SIZE);
        fromSavings->amount = 0;
        memcpy(fromSavings->meta_data, meta_data, METADATA_SIZE);
        toChecking->amount = sum;
        memcpy(toChecking->meta_data, meta_data, METADATA_SIZE);
        status.validation_pass = true;
        status.commit = true;
        return status;
}

OCCSmallBank::WriteCheck::WriteCheck(uint64_t customer, long amount,
                                     char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        AddReadKey(SAVINGS, customer, false);
        AddReadKey(CHECKING, customer, true);
        AddWriteKey(CHECKING, customer);
}

occ_txn_status OCCSmallBank::WriteCheck::Run()
{
        occ_txn_status status;
        SmallBankRecord *checking;
        long sum, balance;
        balance = ((SmallBankRecord*)readset[1].GetValue())->amount;
        sum = 0;
        sum += ((SmallBankRecord*)readset[0].GetValue())->amount;
        sum += ((SmallBankRecord*)readset[1].GetValue())->amount;
        sum -= amount;
        if (sum < 0)
                amount += 1;
        checking = (SmallBankRecord*)writeset[0].GetValue();
        checking->amount = balance - amount;
        memcpy(checking->meta_data, meta_data, METADATA_SIZE);
        status.validation_pass = true;
        status.commit = true;
        return status;
}


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
  return true;
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

    AddWriteKey(SAVINGS, i, false);
    AddWriteKey(CHECKING, i, false);
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

MVSmallBank::Balance::Balance(uint64_t customer, char *meta_data) : Action() {
  this->totalBalance = 0;
  this->meta_data = meta_data;
  AddReadKey(CHECKING, customer);
  AddReadKey(SAVINGS, customer);
}

// Read the customer's checking and savings balance, and add their sum to
// "totalBalance"
bool MVSmallBank::Balance::Run() {
  SmallBankRecord *checkingBalance = (SmallBankRecord*)Read(0); 
  SmallBankRecord *savingsBalance = (SmallBankRecord*)Read(1);
  this->totalBalance = checkingBalance->amount + savingsBalance->amount;
  uint32_t meta_int_sz = METADATA_SIZE/4;
  uint32_t *checking_meta = (uint32_t*)checkingBalance->meta_data;
  uint32_t *savings_meta = (uint32_t*)savingsBalance->meta_data;
  for (uint32_t i = 0; i < meta_int_sz; ++i) {
          meta_data[i] = checking_meta[i];
          meta_data[i] += savings_meta[i];
  }
  return true;
}

MVSmallBank::DepositChecking::DepositChecking(uint64_t customer, long amount,
                                              char *meta_data)
  : Action() {
  this->amount = amount;
  this->meta_data = meta_data;
  AddWriteKey(CHECKING, customer, true);
}

// Deposit "amount" into the customer's checking account.
bool MVSmallBank::DepositChecking::Run() {
  SmallBankRecord *oldCheckingBalance = (SmallBankRecord*)ReadWrite(0);
  SmallBankRecord *newCheckingBalance = (SmallBankRecord*)GetWriteRef(0);
  newCheckingBalance->amount = oldCheckingBalance->amount + amount;
  memcpy(newCheckingBalance->meta_data, meta_data, METADATA_SIZE);
  return true;
}

MVSmallBank::TransactSaving::TransactSaving(uint64_t customer, long amount, 
                                            char *meta_data)
  : Action() {
  this->amount = 0;
  this->meta_data = meta_data;
  AddWriteKey(SAVINGS, customer, true);
}

bool MVSmallBank::TransactSaving::Run() {
  assert(__writeset.size() == 1);
  SmallBankRecord *oldSavings = (SmallBankRecord*)ReadWrite(0);
  SmallBankRecord *savings = (SmallBankRecord*)GetWriteRef(0);
  savings->amount = oldSavings->amount + this->amount;
  memcpy(savings->meta_data, meta_data, METADATA_SIZE);
  return true;
}

MVSmallBank::Amalgamate::Amalgamate(uint64_t fromCustomer, uint64_t toCustomer, 
                                    char *meta_data)
  : Action() {
        this->meta_data = meta_data;
        AddWriteKey(CHECKING, fromCustomer, true);
        AddWriteKey(SAVINGS, fromCustomer, true);
        AddWriteKey(CHECKING, toCustomer, true);
}

bool MVSmallBank::Amalgamate::Run() {
  assert(__writeset.size() == 3);
  SmallBankRecord *fromChecking, *fromSavings, *toChecking;
  long sum = 0;
  sum += ((SmallBankRecord*)ReadWrite(0))->amount;
  sum += ((SmallBankRecord*)ReadWrite(1))->amount;
  long oldChecking = ((SmallBankRecord*)ReadWrite(2))->amount;
  fromChecking = (SmallBankRecord*)GetWriteRef(0);
  fromSavings = (SmallBankRecord*)GetWriteRef(1);
  toChecking = (SmallBankRecord*)GetWriteRef(2);
  fromChecking->amount = 0;
  fromSavings->amount = 0;
  toChecking->amount = sum+oldChecking;
  memcpy(fromChecking->meta_data, meta_data, METADATA_SIZE);
  memcpy(fromSavings->meta_data, meta_data, METADATA_SIZE);
  memcpy(toChecking->meta_data, meta_data, METADATA_SIZE);
  return true;
}

MVSmallBank::WriteCheck::WriteCheck(uint64_t customer, long amount, 
                                    char *meta_data) {
  this->amount = amount;
  this->meta_data = meta_data;
  AddReadKey(SAVINGS, customer);
  AddWriteKey(CHECKING, customer, true);
}

bool MVSmallBank::WriteCheck::Run() {
  assert(__readset.size() == 2);
  assert(__writeset.size() == 1);
  long sum = 0;
  sum += ((SmallBankRecord*)Read(0))->amount;
  sum += ((SmallBankRecord*)ReadWrite(0))->amount;
  sum -= amount;
  if (sum < 0) {
    amount += 1;
  }
  ((SmallBankRecord*)GetWriteRef(0))->amount -= amount;
  memcpy(((SmallBankRecord*)GetWriteRef(0))->meta_data, meta_data,
         METADATA_SIZE);
  return true;
}
