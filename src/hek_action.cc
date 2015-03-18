#include <hek_action.h>

void* hek_action::Read(uint32_t i)
{
        assert(i < readset.size());
        return readset[i].value->value;
}

void* hek_action::GetWriteRef(uint32_t i)
{
        assert(i < writeset.size());
        return writeset[i].value->value;
}

hek_status hek_rmw_action::Run()
{
        uint32_t i, j, num_reads, num_writes;
        uint64_t counter;
        hek_status temp = {true, true};

        num_reads = readset.size();
        num_writes = writeset.size();
        counter = 0;
        for (i = 0; i < num_reads; ++i) {
                char *field_ptr = (char*)Read(i);
                for (j = 0; j < 10; ++j) 
                        counter += *((uint64_t*)&field_ptr[j*100]);
                memcpy(GetWriteRef(i), field_ptr, 1000);
        }
        for (i = 0; i < num_writes; ++i) {
                char *write_ptr = (char*)GetWriteRef(i);
                for (j = 0; j < 10; ++j) {
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
                }
        }
        return temp;        
}

hek_status hek_readonly_action::Run()
{
        /*
        uint32_t num_reads, i;
        num_reads = readset.size();
        for (i = 0; i < num_reads; ++i) {
                if (i == 0) {

                        
                } else {

                }
        }
        */
        hek_status temp = {true, true};
        return temp;
}
