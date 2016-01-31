#include <hek_action.h>
#include <small_bank.h>

extern uint32_t GLOBAL_RECORD_SIZE;

void* hek_action::read(uint64_t key, uint32_t table_id)
{
        uint32_t i, sz;

        sz = readset.size();
        for (i = 0; i < sz; ++i) {
                if (readset[i].key == key && readset[i].table_id == table_id)
                        return readset[i].value->value;
        }
        assert(false);
}

void* hek_action::write_ref(uint64_t key, uint32_t table_id)
{
        uint32_t i, sz;
        void *read_val;

        sz = writeset.size();
        for (i = 0; i < sz; ++i) {
                if (writeset[i].key == key && writeset[i].table_id == table_id) {
                        if (writeset[i].is_rmw == true) {
                                read_val = read(key, table_id);
                                memcpy(writeset[i].value->value, read_val, GLOBAL_RECORD_SIZE);
                        }
                        return writeset[i].value->value;
                }

        }
        assert(false);
}

hek_status hek_action::Run()
{
        hek_status ret = {true, true};
        t->Run();
        return ret;
}

int hek_action::rand()
{
        return 0;
}

/*

hek_status hek_rmw_action::Run()
{
        uint32_t i, j, num_reads, num_writes, num_fields;
        uint64_t counter;
        hek_status temp = {true, true};


        //        for (uint32_t i = 0; i < 10000; ++i) {
        //                single_work();
        //        }


        num_reads = readset.size();
        num_writes = writeset.size();
        counter = 0;
        num_fields = YCSB_RECORD_SIZE / 100;
        for (i = 0; i < num_reads; ++i) {
                char *field_ptr = (char*)Read(i);
                for (j = 0; j < num_fields; ++j) 
                        counter += *((uint64_t*)&field_ptr[j*100]);
        }
        for (i = 0; i < num_writes; ++i) {
                char *write_ptr = (char*)GetWriteRef(i);
                memcpy(write_ptr, Read(i), YCSB_RECORD_SIZE);
                for (j = 0; j < num_fields; ++j) {
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
                }
        }

        return temp;        
}

hek_readonly_action::hek_readonly_action() : hek_action()
{
        readonly = true;

}

hek_status hek_readonly_action::Run()
{
        uint32_t i, j, num_reads;
        assert(readonly == true);
        num_reads = readset.size();
        for (i = 0; i < num_reads; ++i) {
                char *read_ptr = (char*)Read(i);
                for (j = 0; j < 10; ++j) {
                        uint64_t *write_p = (uint64_t*)&reads[j*100];
                        *write_p += *((uint64_t*)&read_ptr[j*100]);
                }                
        }        
        hek_status temp = {true, true};
        return temp;
}

balance::balance(uint64_t customer_id, char *meta_data)
{
        this->total_balance = 0;
        this->meta_data = meta_data;
        struct hek_key key;
        key.written = false;
        key.key = customer_id;
        key.txn = this;
        key.is_rmw = false;
        key.table_id = CHECKING;
        this->readset.push_back(key);
        key.table_id = SAVINGS;
        this->readset.push_back(key);
}

hek_status balance::Run()
{
        hek_status ret = {true, true};
        SmallBankRecord *checking = (SmallBankRecord*)Read(0);
        SmallBankRecord *savings = (SmallBankRecord*)Read(1);
        this->total_balance = checking->amount + savings->amount;
        do_spin();
        return ret;
}

deposit_checking::deposit_checking(uint64_t customer_id, long amount,
                                   char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        struct hek_key key;
        key.written = false;
        key.key = customer_id;
        key.txn = this;
        key.is_rmw = true;
        key.table_id = CHECKING;
        this->readset.push_back(key);
        key.is_rmw = false;
        this->writeset.push_back(key);
}

hek_status deposit_checking::Run()
{
        hek_status ret = {true, true};
        SmallBankRecord *old_rec = (SmallBankRecord*)Read(0);
        SmallBankRecord *new_rec = (SmallBankRecord*)GetWriteRef(0);
        new_rec->amount = old_rec->amount + this->amount;
        do_spin();
        return ret;
}

transact_saving::transact_saving(uint64_t customer_id, long amount,
                                 char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        struct hek_key key;
        key.written = false;
        key.key = customer_id;
        key.txn = this;
        key.is_rmw = true;
        key.table_id = SAVINGS;
        this->readset.push_back(key);
        key.is_rmw = false;
        this->writeset.push_back(key);
}

hek_status transact_saving::Run()
{
        hek_status ret = {true, true};
        SmallBankRecord *read = (SmallBankRecord*)Read(0);
        SmallBankRecord *write = (SmallBankRecord*)GetWriteRef(0);
        write->amount = read->amount + this->amount;
        do_spin();
        return ret;
}

amalgamate::amalgamate(uint64_t from_customer, uint64_t to_customer,
                       char *meta_data)
{
        this->meta_data = meta_data;
        struct hek_key key;
        key.written = false;
        key.txn = this;
        key.is_rmw = true;

        key.key = from_customer;
        key.table_id = CHECKING;
        this->readset.push_back(key);
        this->writeset.push_back(key);
        key.table_id = SAVINGS;
        this->readset.push_back(key);
        this->writeset.push_back(key);

        key.key = to_customer;
        key.table_id = CHECKING;
        this->readset.push_back(key);
        this->writeset.push_back(key);
}

hek_status amalgamate::Run()
{
        hek_status ret = {true, true};
        SmallBankRecord *from_checking = (SmallBankRecord*)Read(0);
        SmallBankRecord *from_savings = (SmallBankRecord*)Read(1);
        SmallBankRecord *to_checking = (SmallBankRecord*)Read(2);

        SmallBankRecord *from_checking_write = (SmallBankRecord*)GetWriteRef(0);
        SmallBankRecord *from_savings_write = (SmallBankRecord*)GetWriteRef(1);
        SmallBankRecord *to_checking_write = (SmallBankRecord*)GetWriteRef(2);

        to_checking_write->amount = from_checking->amount + from_savings->amount
                + to_checking->amount;
        from_checking_write->amount = 0;
        from_savings_write->amount = 0;
        do_spin();
        return ret;
}

write_check::write_check(uint64_t customer_id, long amount, char *meta_data)
{
        this->amount = amount;
        this->meta_data = meta_data;
        struct hek_key key;
        key.written = false;
        key.txn = this;
        key.is_rmw = true;
        key.key = customer_id;
        key.table_id = CHECKING;
        this->readset.push_back(key);
        this->writeset.push_back(key);
        key.is_rmw = false;
        key.table_id = SAVINGS;
        this->readset.push_back(key);
}

hek_status write_check::Run()
{
        hek_status ret = {true, true};
        long sum, balance;
        SmallBankRecord *read_checking = (SmallBankRecord*)Read(0);
        SmallBankRecord *read_savings = (SmallBankRecord*)Read(1);
        SmallBankRecord *write_checking = (SmallBankRecord*)GetWriteRef(0);
        
        balance = read_checking->amount;
        sum = read_checking->amount + read_savings->amount;
        sum -= amount;
        if (sum < 0)
                amount += 1;
        write_checking->amount = balance - amount;
        do_spin();
        return ret;
}
*/
