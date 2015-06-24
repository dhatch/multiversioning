#include <db.h>
#include <setup_workload.h>
#include <config.h>
#include <record_generator.h>
#include <uniform_generator.h>
#include <zipf_generator.h>
#include <ycsb.h>
#include <small_bank.h>
#include <set>
#include <common.h>

uint64_t gen_unique_key(RecordGenerator *gen,
                        std::set<uint64_t> *seen_keys)
{
        while (true) {
                uint64_t key = gen->GenNext();
                if (seen_keys->find(key) == seen_keys->end()) {
                        seen_keys->insert(key);
                        return key;
                }
        }
}


txn* generate_small_bank_action(uint32_t num_records, bool read_only)
{
        txn *t;
        int mod, txn_type;
        long amount;
        uint64_t customer, from_customer, to_customer;
        
        if (read_only == true) 
                mod = 1;
        else 
                mod = 5;        
        txn_type = rand() % mod;
        if (txn_type == 0) {
                customer = (uint64_t)(rand() % num_records);
                t = new SmallBank::Balance(customer);
        } else if (txn_type == 1) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                t = new SmallBank::DepositChecking(customer, amount);
        } else if (txn_type == 2) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                t = new SmallBank::TransactSaving(customer, amount);
        } else if (txn_type == 3) {
                from_customer = (uint64_t)(rand() % num_records);
                do {
                        to_customer = (uint64_t)(rand() % num_records);
                } while (to_customer == from_customer);
                t = new SmallBank::Amalgamate(from_customer,
                                                     to_customer);
        } else if (txn_type == 4) {
                customer = (uint64_t)(rand() % num_records);
                amount = (long)(rand() % 25);
                if (rand() % 2 == 0) {
                        amount *= -1;
                }
                t = new SmallBank::WriteCheck(customer, amount);
        } else {
                assert(false);
        }
        return t;
}

txn* generate_ycsb_readonly(RecordGenerator *gen, workload_config config)
{
        using namespace std;
        
        uint32_t i;
        uint64_t key;
        txn *ret;
        vector<uint64_t> ctor_keys;
        set<uint64_t> seen_keys;

        /* Generate the read-set. */
        ret = NULL;
        for (i = 0; i < config.read_txn_size; ++i) {
                if (i < 10)
                        key = gen_unique_key(gen, &seen_keys);
                else
                        key = gen_unique_key(gen, &seen_keys);
                ctor_keys.push_back(key);
        }

        /* Generate the txn. */
        ret = new ycsb_readonly(ctor_keys);
        assert(ret != NULL);
        assert(ret->num_reads() == ctor_keys.size());
        assert(ret->num_writes() == 0);
        assert(ret->num_rmws() == 0);
        return ret;
}

txn* generate_ycsb_rmw(RecordGenerator *gen, uint32_t num_reads,
                       uint32_t num_rmws)
{
        using namespace std;
        
        uint32_t i;
        uint64_t key;
        set<uint64_t> seen_keys;
        vector<uint64_t> reads, rmws;
        txn *ret;

        /* Generate the txn's read- and write-sets. */
        ret = NULL;
        for (i = 0; i < num_rmws; ++i) {
                key = gen_unique_key(gen, &seen_keys);
                rmws.push_back(key);
        }

        for (i = 0; i < num_reads; ++i) {
                key = gen_unique_key(gen, &seen_keys);
                reads.push_back(key);
        }

        /* Create a txn to return. */
        ret = new ycsb_rmw(reads, rmws);
        assert(ret != NULL);
        assert(ret->num_reads() == reads.size());
        assert(ret->num_rmws() == rmws.size());
        assert(ret->num_writes() == 0);
        return ret;
}

txn* generate_ycsb_action(RecordGenerator *gen, workload_config config)
{
        uint32_t num_reads, num_rmws, num_writes;
        int flip;
        //assert(RMW_COUNT <= config.txnSize);
        flip = (uint32_t)rand() % 100;
        assert(flip >= 0 && flip < 100);
        if (flip < config.read_pct) {
                return generate_ycsb_readonly(gen, config);
        } else if (config.experiment == 0) {
                num_writes = 0;
                num_rmws = config.txn_size;
                num_reads = 0;
        } else if (config.experiment == 1) {
                num_writes = 0;
                num_rmws = RMW_COUNT;
                num_reads = config.txn_size - RMW_COUNT;
        } else if (config.experiment == 2) {
                assert(false);
        }
        return generate_ycsb_rmw(gen, num_rmws, num_reads);
}

txn** generate_small_bank_input(workload_config conf)
{
        using namespace SmallBank;
        
        uint32_t num_txns, i, remainder;
        uint64_t start, end;
        txn **ret;

        /* Each txn performs 1000 insertions. */
        num_txns = conf.num_records / 1000;
        remainder = conf.num_records % 1000;
        ret = (txn**)malloc(sizeof(txn*)*num_txns);
        for (i = 0; i < num_txns; ++i) {
                start = 1000*i;
                end = start + 1000;
                if (i == num_txns - 1)
                        end += remainder;
                ret[i] = new LoadCustomerRange(start, end);
        }
        return ret;
}

txn** generate_ycsb_input(workload_config conf)
{
        using namespace SmallBank;
        uint32_t num_txns, i, remainder;
        uint64_t start, end;
        txn **ret;

        /* Each txn performs 1000 insertions. */
        num_txns = conf.num_records / 1000;
        remainder = conf.num_records % 1000;
        ret = (txn**)malloc(sizeof(txn*)*num_txns);
        for (i = 0; i < num_txns; ++i) {
                start = 1000*i;
                end = start + 1000;
                if (i == num_txns - 1)
                        end += remainder;
                ret[i] = new ycsb_insert(start, end);
        }
}

uint32_t generate_input(workload_config conf, txn ***loaders)
{
        if (conf.experiment == 3 || conf.experiment == 4) {
                generate_small_bank_input(conf);
        } else if (conf.experiment < 3) {

        } else {
                assert(false);
        }
}

txn* generate_transaction(workload_config config)
{
        RecordGenerator *gen;
        txn *txn;
        if (config.experiment == 3) 
                txn = generate_small_bank_action(config.num_records, false);
        else if (config.experiment == 4) 
                txn = generate_small_bank_action(config.num_records, true);
        else if (config.experiment < 3)                
                txn = generate_ycsb_action(gen, config);
        else 
                assert(false);        
        return txn;
}
