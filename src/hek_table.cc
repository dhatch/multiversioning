#include <cpuinfo.h>
#include <hek_table.h>
#include <hek_action.h>

hek_table::hek_table(uint64_t num_slots, int cpu_start, int cpu_end)
{
        this->num_slots = num_slots;
        this->slots =
                (hek_table_slot*)
                alloc_interleaved(sizeof(hek_table_slot)*num_slots,
                                        cpu_start, cpu_end);
        memset(slots, 0x0, sizeof(hek_table_slot)*num_slots);
}

hek_table_slot* hek_table::get_slot(uint64_t key)
{
        return &slots[key];
}

bool hek_table::get_preparing_ts(hek_record *record, uint64_t *ret)
{
        volatile uint64_t temp;
        //        volatile uint32_t txn_state;
        hek_action *txn;
        
        barrier();
        temp = record->begin;
        barrier();
        if (!IS_TIMESTAMP(temp)) {
                txn = GET_TXN(temp);
                barrier();
                temp = txn->end;
                barrier();
                if (HEK_STATE(temp) >= PREPARING) {
                        *ret = HEK_TIME(temp);
                        return true;
                } else {
                        return false;
                }
        } else {
                *ret = HEK_TIME(temp);
                return true;
        }
}

hek_record* hek_table::search_stable(uint64_t key, uint64_t ts,
                                     hek_record *iter)
{
        while (iter != NULL) {
                assert(IS_TIMESTAMP(iter->begin));
                if (key == iter->key && HEK_TIME(iter->begin) < ts)
                        break;
                iter = iter->next;
        }
        return iter;
}

bool hek_table::validate(hek_record *cur, hek_record *prev)
{
        if (IS_TIMESTAMP(cur->begin) || (prev == NULL) ||
            IS_TIMESTAMP(prev->begin))
                return true;
        return false;
}

hek_record* hek_table::search_bucket(uint64_t key, uint64_t ts,
                                     struct hek_table_slot *slot)
{
        hek_record *cur, *prev;
        uint64_t record_ts;
        while (true) {
                barrier();
                cur = (hek_record*)slot->records;
                barrier();
                if (cur != NULL) {
                        prev = cur->next;
                } else {
                        prev = NULL;
                        break;
                }
                if (validate(cur, prev))
                        break;
        }
        if (cur == NULL)
                return NULL;
        if (get_preparing_ts(cur, &record_ts) && record_ts < ts) {
                return cur;
        } else {
                return search_stable(key, ts, prev);
        }
}

hek_record* hek_table::get_version(uint64_t key, uint64_t ts)
{
        struct hek_table_slot *slot;
        slot = get_slot(key);
        return search_bucket(key, ts, slot);
}

bool hek_table::insert_version(hek_record *record)
{
        hek_table_slot *slot;
        hek_record *prev;

        slot = get_slot(record->key);
        if (try_lock((volatile uint64_t*)&slot->latch)) {
                prev = (hek_record*)slot->records;
                prev->end = record->begin;
                record->next = (hek_record*)slot->records;
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

        slot = get_slot(record->key);
        assert(slot->latch == 1 && slot->records == record);
        prev = record->next;
        slot->records = prev;
        if (prev != NULL)
                prev->end = ts;
        xchgq(&slot->latch, 0x0);
}

void hek_table::finalize_version(hek_record *record, uint64_t ts)
{
        hek_table_slot *slot;
        hek_record *prev;

        slot = get_slot(record->key);
        assert(slot->latch == 1 && slot->records == record);
        record->begin = ts;
        prev = record->next;
        if (prev != NULL) 
                prev->end = ts;
        xchgq(&slot->latch, 0x0);
}
