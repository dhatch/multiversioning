#include <locking_action.h>
#include <algorithm>

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
}

locking_key::locking_key()
{
}

locking_action::locking_action(txn *txn) : translator(txn)
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

int locking_action::find_key(uint64_t key, uint32_t table_id,
                             std::vector<locking_key> key_list)
{
        uint32_t i, num_keys;
        int ret;

        ret = -1;
        num_keys = key_list.size();
        for (i = 0; i < num_keys; ++i) {
                if (key_list[i].key == key &&
                    key_list[i].table_id == table_id) {
                        ret = i;
                        break;
                }
        }
        return ret;
}

void* locking_action::write_ref(uint64_t key, uint32_t table_id)
{
        locking_key *k;
        int index;
        
        index = find_key(key, table_id, this->writeset);
        assert(index != -1 && index < this->writeset.size());
        k = &this->writeset[index];
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
}

void* locking_action::read(uint64_t key, uint32_t table_id)
{
        locking_key *k;
        int index;
        
        index = find_key(key, table_id, this->readset);
        assert(index != -1 && index < this->readset.size());
        k = &this->readset[index];
        if (k->value == NULL) 
                k->value = lookup(k);
        return k->value;
}

void locking_action::prepare()
{
        if (this->prepared == true) 
                return;
        std::sort(this->readset.begin(), this->readset.end());
        std::sort(this->writeset.begin(), this->writeset.end());
        this->prepared = true;
}

bool locking_action::Run()
{
        return this->t->Run();
}
