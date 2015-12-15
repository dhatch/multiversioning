// Author: Jose M. Faleiro (faleiro.jose.manuel@gmail.com)
// CACHE_PAD macro stolen from Corey (http://pdos.csail.mit.edu/corey/) 
//

#ifndef CONCURRENT_QUEUE_HH
#define CONCURRENT_QUEUE_HH

#include <stdint.h>
#include <cstddef>
#include <iostream>
#include <cassert>
#include <cstring>

#include "util.h"
#include "machine.h"


#define CACHE_PAD 64
#define PAD(t)								\
	union __attribute__((__packed__,  __aligned__(CACHE_PAD))) {        \
        t v;                                                            \
        char __p[CACHE_PAD + (sizeof(t) / CACHE_PAD) * CACHE_PAD];      \
	}




// 
// An implementation of lock-free queues for inter-thread communication. We 
// avoid using locks+signals for two reasons: (1) (potential) syscall overhead,
// (2) larger cache footprint of locking based queues. 
// 

struct queue_elem {
    uint64_t m_data;						// Pointer to data (app specific).
    volatile struct queue_elem* m_next;
} __attribute__((aligned(64)));


template<class T>
class SimpleQueue {
 public:
    char* m_values;
    uint64_t m_size;
    volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) m_head;    
    volatile uint64_t __attribute__((__packed__, __aligned__(CACHE_LINE))) m_tail;    

    SimpleQueue(char* values, uint64_t size) {
        m_values = values;
        m_size = (uint64_t)size;
        assert(!(m_size & (m_size-1)));
        assert(sizeof(T) < CACHE_LINE);
        memset(values, 0x0, m_size*CACHE_LINE);
        m_head = 0;
        m_tail = 0;        
        barrier();
    }
    
    uint64_t diff() {
        return m_tail - m_head;
    }
        
    
    bool isEmpty() {
        return m_head == m_tail;
    }

    bool Enqueue(T data) {
        assert(m_head >= m_tail);
        if (m_head == m_tail + m_size) {
            return false;
        }
        else {
            uint64_t index = m_head & (m_size-1);
            assert(index < m_size);
            
            (*(T*)&m_values[index*CACHE_LINE]) = data;
            fetch_and_increment(&m_head);
            return true;
        }
    }
    
    void EnqueueBlocking(T data) {
            uint64_t head = m_head;
            uint64_t tail;
            assert(m_head >= m_tail);
            while (true) {
                    barrier();
                    tail = m_tail;
                    barrier();
                    if (head < tail + m_size)
                            break;
            }
            //        while (m_head == m_tail + m_size) 
            //            ;
        uint64_t index = head & (m_size - 1);
        assert(index < m_size);
        assert(index <= ((m_size - 1) << 6));
        (*(T*)&m_values[index*CACHE_LINE]) = data;
        fetch_and_increment(&m_head);
    }
    
    T DequeueBlocking() {
            uint64_t head, tail;
            tail = m_tail;
        assert(m_head >= m_tail);
        while (true) {
                barrier();
                head = m_head;
                barrier();
                if (head > tail)
                        break;
        }
        //        while (m_head == m_tail) 
        //            ;
        uint64_t index = tail & (m_size - 1);
        assert(index < m_size);
        assert(index <= ((m_size - 1) << 6));
        T ret = (*(T*)&m_values[index*CACHE_LINE]);
        fetch_and_increment(&m_tail);
        return ret;
    }

    bool Dequeue(T* value) {
        assert(m_head >= m_tail);
        if (m_head == m_tail) {
            return false;
        }
        else {
            uint64_t index = m_tail & (m_size - 1);            
            assert(index < m_size);
            assert(index <= ((m_size - 1) << 6));
            *value = (*(T*)&m_values[index*CACHE_LINE]);
            fetch_and_increment(&m_tail);
            return true;
        }
    }
};

class ConcurrentQueue {

  volatile struct queue_elem* __attribute__((aligned(64))) m_head;
  volatile struct queue_elem* __attribute__((aligned(64))) m_tail;

  inline bool
  cmp_and_swap(volatile struct queue_elem* volatile *  to_write, 
	       volatile struct queue_elem* to_cmp,
	       volatile struct queue_elem* new_value) {
	volatile struct queue_elem* out;
	asm volatile ("lock; cmpxchgq %2, %1;"
				  : "=a" (out), "+m" (*to_write)
				  : "q" (new_value), "0" (to_cmp)
				  : "cc", "memory");
	return (out == to_cmp);
  }
  
public:
  ConcurrentQueue(volatile struct queue_elem* dummy) {
	m_head = dummy;
	m_tail = dummy;
  }

  void
  Enqueue(volatile struct queue_elem* elem) {

	volatile struct queue_elem* tail;
	volatile struct queue_elem* next;
        elem->m_next = NULL;
	while (true) {
	  tail = m_tail;
	  next = tail->m_next;
	  
	  // Check if we still have a valid reference to the tail. 
	  // If this fails, then someone else added an element in the meanwhile. 
	  if (tail == m_tail) {	
		// Check if we're about to manipulate the last element of the list. 
		// If this fails, then we have to necessarily retry. 
		if (next == NULL) {	
		  // Try to add a new element to the queue. 
		  if (cmp_and_swap(&(tail->m_next), next, elem)) {
			break;
		  }
		}	  
		else {
		  
		  // If we're here, it means that tail wasn't pointing to the last 
		  // element of the list. 
		  cmp_and_swap(&m_tail, tail, next);
		}
	  }
	}	
	// Finally, try to make tail point to the newly added element. 
	cmp_and_swap(&m_tail, tail, elem);
  }  

      

  volatile struct queue_elem*
  Dequeue(bool block) {
	volatile struct queue_elem* head = NULL;
	volatile struct queue_elem* tail = NULL;
	volatile struct queue_elem* next = NULL;
	uint64_t result;

	while (true) {
	  head = m_head;
	  tail = m_tail;
	  next = head->m_next;
	  
	  // Make sure that head still points to the head. 
	  if (head == m_head) {
		
              // The head and the tail are the same, this means that the 
              // queue is 
              // empty, or the tail hasn't been advanced properly. 
              if (head == tail) {
		  
		  // If next is NULL, it means we've reached the end of the list. If 
		  // that's the case, we need to keep re-trying until it becomes 
		  // non-NULL. If it's not NULL, try to move the tail forward. 
		  if (next != NULL) {

			// Try to move the tail forward. 
			cmp_and_swap(&m_tail, tail, tail->m_next);
		  }

                  if (block) {
                      do_pause();
                      continue;
                  }
                  else {
                      return NULL;
                  }
		}
		else {
		  if (next != NULL) {
			result = next->m_data;
			if (cmp_and_swap(&m_head, head, next)) {
			  break;
			}
		  }
		}		
	  }
	}
	head->m_next = NULL;
	head->m_data = result;
	return head;
  }
};

// 
// Multiple Producer, Single Consumer Queue. 
//
class MPSCQueue {
  PAD(volatile struct queue_elem*) m_head;
  PAD(volatile struct queue_elem*) m_tail;

public:
  MPSCQueue(volatile struct queue_elem* dummy) {
	dummy->m_next = NULL;
	dummy->m_data = 0;
	m_tail.v = dummy;
	m_head.v = dummy;
  }

  //
  // Enqueue is similar to enqueuing an element to the logical queue of waiters
  // in an MCS lock (the xchg is unconditional). 
  //
  inline void 
  Enqueue(volatile struct queue_elem* cur) {	
    cur->m_next = NULL;
	volatile struct queue_elem *prev_head;
    asm volatile("movq %2, %%r10\n\t"
                 "lock; xchgq %1, %%r10\n\t"
                 "movq %%r10, %0"
                 : "=a" (prev_head), "+m" (m_head.v)
                 : "q" (cur)
                 : "%r10", "memory", "cc");
	prev_head->m_next = cur;
  }
  
  //
  // The returned struct queue_elem* should *NOT* be recycled. Garbage is set to
  // the pointer to the node we wish to recycle. Ugly, but can't be helped :(. 
  //
  inline volatile struct queue_elem*
  Dequeue(volatile struct queue_elem** garbage) {
	// Spin until something new has been enqueued.
	while ((m_tail.v)->m_next == NULL) {
	  asm volatile("lfence":::"memory");
	}
	*garbage = (m_tail.v);
	m_tail.v = (m_tail.v)->m_next;
	return (m_tail.v);
  }
};

class SPMCQueue {
  PAD(volatile struct queue_elem*) m_head;
  PAD(volatile struct queue_elem*) m_tail;

  // 
  // Compare to_swap with expected_value, if they are equal, set to_swap to 
  // new _value.
  //
  inline bool
  cmp_and_swap(volatile struct queue_elem* expected_value,
               volatile struct queue_elem* new_value) {
    struct queue_elem* out;
    asm volatile ("lock; cmpxchgq %2, %1;"
                  : "=a" (out), "+m" (m_tail.v)
                  : "q" (new_value), "0"(expected_value)
                  : "cc", "memory");
	return (out == expected_value);
  }

public:
  SPMCQueue() {
	m_head.v = NULL;
	m_tail.v = NULL;
  }
 
  void
  Traverse() {
	volatile struct queue_elem* temp = (m_tail.v)->m_next;
	while (temp != NULL) {
	  std::cout << temp->m_data << "\n";
	  temp = temp->m_next;
	}
  }
  
  //
  // We have only a single producer, so there's no need to use atomic 
  // instructions here. 
  //
  inline void 
  Enqueue(volatile struct queue_elem* cur) {	
    cur->m_next = NULL;
	if ((m_head.v) != NULL) {
	  (m_head.v)->m_next = cur;
	}
	(m_head.v) = cur;
	if ((m_tail.v) == NULL) {
	  (m_tail.v) = cur;
	}
  }
  
  // 
  // Blocking implementation of Dequeue(). Caller will spin until it obtains a 
  // new element. Caller does not make a syscall or block in the kernel (unless
  // preempted).
  //
  inline volatile struct queue_elem* 
  Dequeue() {
    volatile struct queue_elem* cmp;
    while (true) {
      
      // Make sure that the queue is non-empty. 
	  cmp = m_tail.v;
      if (cmp != NULL) {
		
		// Try to atomically grab the element at the tail. 
		if (cmp_and_swap(cmp, cmp->m_next)) {
		  return cmp;
		}
      }
    }
  }    
};

//
// Use this class to get new queue_elems for inter-thread communication. The 
// elements are pre-allocated, so we can avoid using malloc/new and the badness
// associated with calls to malloc/new.
//
// ElementStore is meant to be used for thread-local storage of queue_elems. 
// Calls to getNew and returnElem are unsynchronized. 
//
class ElementStore {
private:
  volatile struct queue_elem* free_list_start;
  volatile struct queue_elem* free_list_end;
  
public:
  ElementStore(int num_elems) {
    free_list_start = new struct queue_elem[num_elems];
    for (int i = 0; i < num_elems; ++i) {
      free_list_start[i].m_data = 0;
      free_list_start[i].m_next = &free_list_start[i+1];
    }
    free_list_start[num_elems-1].m_next = NULL;
	free_list_end = &free_list_start[num_elems-1];
  }

  volatile struct queue_elem*
  getNew() {
    // XXX: We need an assertion to make sure that the free_list is not empty. 
    volatile struct queue_elem* ret = free_list_start;
    free_list_start = free_list_start->m_next;
    ret->m_next = NULL;
    return ret;
  }
  
  void
  returnElem(volatile struct queue_elem* elem) {
      free_list_end->m_next = elem;
      free_list_end = elem;
      elem->m_next = NULL;
      elem->m_data = 0;
  }
};

#endif // CONCURRENT_QUEUE_HH
