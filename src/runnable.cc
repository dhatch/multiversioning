#include <runnable.hh>
#include <util.h>
#include <cpuinfo.h>

Runnable::Runnable(int cpu_number) {
    m_start_signal = 0;
    m_cpu_number = cpu_number;
}

void
Runnable::Run() {
    assert(m_start_signal == 0);
    
    // Kickstart the worker thread
    pthread_create(&m_thread, NULL, Bootstrap, this);
    
    // Wait for the newly spawned thread to report that it has successfully
    // initialized
    while (!m_start_signal)
        ;    
}

void*
Runnable::Bootstrap(void *arg) {
    Runnable *worker = (Runnable*)arg;
    
    // Pin the thread to a cpu
    if (pin_thread((int)worker->m_cpu_number) == -1) {
        std::cout << "Couldn't bind to a cpu!\n";
        exit(-1);
    }
    
    worker->Init();
    
    // Signal that we've initialized
    fetch_and_increment(&worker->m_start_signal);	

	// Start the runnable thread
    worker->StartWorking();
    return NULL;
}
