#include <runnable.hh>
#include <util.h>
#include <cpuinfo.h>
#include <pthread.h>
#include <stdlib.h>

#define PRNG_BUFSZ 32

void Runnable::rand_init()
{
        char *random_buf;
        
        m_rand_state = (struct random_data*)malloc(sizeof(struct random_data));
        memset(m_rand_state, 0x0, sizeof(struct random_data));
        random_buf = (char*)malloc(PRNG_BUFSZ);
        memset(random_buf, 0x0, PRNG_BUFSZ);
        initstate_r(random(), random_buf, PRNG_BUFSZ, m_rand_state);
}

Runnable::Runnable(int cpu_number) {
        m_start_signal = 0;
        m_cpu_number = cpu_number;
        rand_init();
}

int Runnable::gen_random()
{
        int ret;
        random_r(m_rand_state, &ret);
        return ret;
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
