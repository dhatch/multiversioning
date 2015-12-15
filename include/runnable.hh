#ifndef                 RUNNABLE_HH_
#define                 RUNNABLE_HH_

#include <pthread.h>
#include <cassert>
#include <iostream>

class Runnable {

private:
        void rand_init();

protected:
        volatile uint64_t                     m_start_signal;
        int                                   m_cpu_number;
        pthread_t                             m_thread;
        uint64_t                              m_pthreadId;
        struct random_data *m_rand_state;

  virtual void
  StartWorking() = 0;

  virtual void
  Init() = 0;
  
  static void*
  Bootstrap(void *arg);




public:    
  Runnable(int cpu_number);
    
  void
  Run();

        void WaitInit();

        virtual int gen_random();

};

#endif          //  RUNNABLE_HH_
