#include <hek_table.h>

struct hek_table_slot {
        volatile uint64_t latch;
        volatile hek_record *records;
};

hek_table_slot* hek_table::get_slot(uint64_t key)
{
        return NULL;
}

hek_record* hek_table::search_bucket(uint64_t key, uint64_t ts,
                                     struct hek_table_slot *slot)
{
        return NULL;
        /*
        hek_record *cur, *prev;
        cur = slot->records;
        if (cur != NULL)
                prev = cur->next;
        while (true) {
                if (cur->begin < ts)
        }
        */
}

hek_record* hek_table::get_version(uint64_t key, uint64_t ts)
{
        struct hek_table_slot *slot;
        slot = get_slot(key);
        return search_bucket(slot);
}

bool hek_table::insert_version(hek_record *record)
{
        hek_table_slot *slot;
        hek_record *prev;
        uint64_t is_locked;
        if (try_lock(slot->latch)) {
                prev = slot->records;
                prev->end = record->begin;
                record->next = slot->records;
                slot->records = record;
                return true;
        } else {
                return false;
        }
}

void hek_table::remove_version(hek_record *record, uint64_t ts)
{
        hek_table_slot *slot;
        hek_record *prev;
        assert(slot->latch == 1 && slot->records == record);
        prev = record->prev;
        slot->records = prev;
        if (prev != NULL)
                prev->end = ts;
        xchgq(&slot->latch, 0x0);
}

void hek_table::finalize_version(hek_record *record, uint64_t ts)
{
        hek_table_slot *slot;
        hek_record *prev;
        assert(slot->latch == 1 && slot->records == record);
        record->begin = ts;
        prev = record->prev;
        if (prev != NULL) 
                prev->end = ts;
        xchgq(&slot->latch, 0x0);
}
