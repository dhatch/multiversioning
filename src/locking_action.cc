#include <locking_action.h>

locking_key::locking_key(uint64_t key, uint32_t table_id, bool is_write)
{
        this->key = key;
        this->table_id = table_id;
        this->is_write = is_write;
        this->dependency = NULL;
        this->is_held = false;
        this->latch = NULL;
        this->prev = NULL;
        this->next = NULL;
        this->is_initialized = false;
        this->value = NULL;
        this->read_index = 0;
        this->write_index = 0;
}

locking_action::locking_action(txn *txn) : db(txn)
{
        this->prepared = false;
}

void locking_action::add_write_key(uint64_t key, uint32_t table_id)
{
        locking_key to_add(key, table_id, true);
        this->writeset.push_back(to_add);
}

void locking_action::add_read_key(uint64_t key, uint32_t table_id)
{
        locking_key to_add(key, table_id, false);
        this->readset.push_back(to_add);
}

void* locking_action::lookup(locking_key *k)
{
        if (k->is_write == true) 
                return this->tables[k->table_id]->GetAlways(k->key);
        else
                return this->tables[k->table_id]->Get(k->key);
}

void* locking_action::write_ref(uint64_t key, uint32_t table_id)
{
        locking_key *k;
        uint32_t i, num_writes;

        for (i = 0; i < num_writes; ++i) {
                if (this->writeset[i].key == key &&
                    this->writeset[i].table_id == table_id) {
                        k = &this->writeset[i];
                        break;
                }
        }
        assert(k != NULL);
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
}

void* locking_action::read(uint64_t key, uint32_t table_id)
{
        locking_key *k;
        uint32_t i, num_reads;

        num_reads = this->readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (this->readset[i].key == key &&
                    this->readset[i].table_id == table_id) {
                        k = &this->readset[i];
                        break;
                }
        }
        assert(k != NULL);
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
}

void locking_action::prepare()
{
        if (this->prepared == true) 
                return;
        std::sort(this->readset.start(), this->readset.end());
        std::sort(this->writeset.start(), this->writeset.end());
        this->prepared = true;
}
