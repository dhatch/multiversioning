#include <hek_action.h>
  
hek_status hek_rmw_action::Run()
{
        hek_status temp = {true, true};
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
