#include <record_buffer.h>
#include <cassert>

/*
 * Create a linked list of thread local buffers for a particular type of record.
 * Given the size of each buffer and the number of buffers.
 */
void RecordBuffers::LinkBufs(struct RecordBuffy *start, uint32_t buf_size,
                             uint32_t num_bufs)
{
        uint32_t offset, i;
        char *cur;
        struct RecordBuffy *temp;

        assert(num_bufs > 0);
        offset = sizeof(struct RecordBuffy*) + buf_size;
        cur = (char*)start;
        for (i = 0; i < num_bufs; ++i) {
                temp  = (struct RecordBuffy*)cur;
                temp->next = (struct RecordBuffy*)(cur + offset);
                cur = (char*)(temp->next);
        }
        temp->next = NULL;
}

void* RecordBuffers::AllocBufs(struct RecordBuffersConfig conf)
{
        uint32_t i;
        uint64_t total_size, single_buf_sz;
        total_size = 0;
        for (i = 0; i < conf.num_tables; ++i) {
                single_buf_sz =
                        sizeof(struct RecordBuffy*)+conf.record_sizes[i];
                total_size += conf.num_buffers * single_buf_sz;
        }
        //        std::cerr << "Record size: " << total_size << "\n";
        return alloc_mem(total_size, conf.cpu);
}

/*
 * 
 */
RecordBuffers::RecordBuffers(struct RecordBuffersConfig conf)
{
        uint32_t i;
        char *temp;
        temp = (char *)alloc_mem(conf.num_tables*sizeof(struct RecordBuffy*),
                                 conf.cpu);
        assert(temp != NULL);
        record_lists = (struct RecordBuffy**)temp;
        temp = (char*)AllocBufs(conf);
        assert(temp != NULL);
        for (i = 0; i < conf.num_tables; ++i) {
                LinkBufs((struct RecordBuffy*)temp, conf.record_sizes[i],
                         conf.num_buffers);
                record_lists[i] = (struct RecordBuffy*)temp;
                temp += conf.record_sizes[i]*conf.num_buffers;
        }
        this->num_records = conf.num_buffers;
}

void* RecordBuffers::GetRecord(uint32_t tableId)
{
        RecordBuffy *ret;
        assert(record_lists[tableId] != NULL && num_records > 0);
        num_records -= 1;
        ret = record_lists[tableId];
        record_lists[tableId] = ret->next;
        ret->next = NULL;
        return ret;
}

void RecordBuffers::ReturnRecord(uint32_t tableId, void *record)
{
        RecordBuffy *ret;
        ret = (RecordBuffy*)record;
        ret->next = record_lists[tableId];
        record_lists[tableId] = ret;
        num_records += 1;
}

