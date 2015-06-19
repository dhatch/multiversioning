#include <db.h>

void* txn::get_write_ref(uint64_t key, uint32_t table_id)
{
        return trans->write_ref(key, table_id);
}

void* txn::get_read_ref(uint64_t key, uint32_t table_id)
{
        return trans->read(key, table_id);
}
