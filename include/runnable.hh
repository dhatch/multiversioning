#ifndef 		RUNNABLE_HH_
#define 		RUNNABLE_HH_

#include <pthread.h>
#include <cassert>
#include <iostream>

class Runnable {
private:
    volatile uint64_t 		m_start_signal;
    int 					m_cpu_number;
    pthread_t 				m_thread;

protected:
    virtual void
    StartWorking() = 0;

    static void*
    Bootstrap(void *arg);

public:    
    Runnable(int cpu_number);
    
    void
    Run();
};

#endif		//  RUNNABLE_HH_
