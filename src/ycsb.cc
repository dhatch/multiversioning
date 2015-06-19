#include <ycsb.h>

ycsb_rmw::ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes)
{
        uint32_t num_reads, num_writes, i;
        for (i = 0; i < num_reads; ++i) 
                this->reads.push_back(reads[i]);
        for (i = 0; i < num_writes; ++i) 
                this->writes.push_back(writes[i]);
}

bool ycsb_rmw::Run()
{
        uint32_t i, j, num_reads, num_writes;
        uint64_t counter;
        char *field_ptr, *write_ptr;

        num_reads = this->reads.size();
        num_writes = this->writes.size();

        /* Accumulate each field of records in the readset into "counter". */
        counter = 0;
        for (i = 0; i < num_reads; ++i) {
                field_ptr = (char*)get_read_ref(reads[i], 0);
                for (j = 0; j < 10; ++j)
                        counter += *((uint64_t*)&field_ptr[j*100]);
        }

        /* Perform an RMW operation on each element of the writeset. */
        for (i = 0; i < num_writes; ++i) {
                write_ptr = (char*)get_write_ref(writes[i], 0);
                for (j = 0; j < 10; ++j)
                        *((uint64_t*)&write_ptr[j*100]) += j+1+counter;
        }
}
