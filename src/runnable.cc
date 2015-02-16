#include <runnable.hh>
#include <util.h>
#include <cpuinfo.h>
#include <pthread.h>

Runnable::Runnable(int cpu_number) {
        m_start_signal = 0;
        m_cpu_number = cpu_number;
}

void
Runnable::Run() {
    assert(m_start_signal == 0);
    
    // Kickstart the worker thread
    pthread_create(&m_thread, NULL, Bootstrap, this);
} 

void Runnable::WaitInit()
{
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
    
  assert((uint32_t)worker->m_thread != 0);
    
  //  worker->m_pthreadId = (uint64_t)pthread_getthreadid_np();
  //  assert(worker->m_pthreadId != 0);
  worker->Init();
    
  // Signal that we've initialized
  fetch_and_increment(&worker->m_start_signal);	

  // Start the runnable thread
  worker->StartWorking();
  return NULL;
}
