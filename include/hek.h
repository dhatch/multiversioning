#ifndef HEK_H_
#define HEK_H_

#include <concurrent_queue.h>
#include <hek_action.h>
#include <runnable.hh>

class hek_table;

class hek_queue {
        volatile hek_action *head;
        volatile hek_action **tail;

 public:
        void* operator new(std::size_t sz, int cpu)
        {
                void *ret;
                ret = alloc_mem(sz, cpu);
                memset(ret, 0x0, sz);
                return ret;
        }
        
        hek_queue();
        virtual void enqueue(hek_action *txn);
        virtual hek_action* dequeue_batch();
};

struct hek_batch {
        hek_action **txns;
        uint32_t num_txns;
};


struct hek_worker_config {
        int cpu;
        volatile uint64_t *global_time;
        uint32_t num_tables;
        uint32_t num_threads;
        hek_table **tables;
        SimpleQueue<hek_batch> *input_queue;
        SimpleQueue<hek_batch> *output_queue;
        hek_queue *commit_queue;
        hek_queue *abort_queue;
        uint64_t *free_list_sizes;
        uint32_t *record_sizes;
};

class hek_worker : public Runnable {
 private:
        uint32_t num_committed;
        uint32_t num_done;
        
        hek_worker_config config;
        
        struct hek_record **records;

        virtual void init_allocator();
        virtual struct hek_record* get_new_record(uint32_t table_id);
        virtual void return_record(uint32_t table_id,
                                   struct hek_record *record);
        
        
        virtual void run_txn(hek_action *txn);
        virtual void get_reads(hek_action *txn);
        virtual void get_writes(hek_action *txn);
        
        virtual bool validate_single(hek_action *txn, hek_key *key);
                             
        virtual bool validate_reads(hek_action *txn);
        //        virtual bool validate(hek_action *txn);

        virtual bool insert_writes(hek_action *txn);                
        virtual void remove_writes(hek_action *txn);
        virtual void install_writes(hek_action *txn);
        virtual void check_dependents();

        virtual void transition_begin(hek_action *txn);
        virtual void transition_preparing(hek_action *txn);
        virtual void transition_commit(hek_action *txn);
        virtual void transition_abort(hek_action *txn);

        virtual void add_commit_dep(hek_action *out, hek_key *key,
                                    hek_action *in);
                                                                        
        
        virtual void do_abort(hek_action *txn);
        virtual void do_commit(hek_action *txn);

        virtual void commit_waiters(hek_action *txn);
        virtual void kill_waiters(hek_action *txn);

        virtual inline void insert_commit_queue(hek_action *txn);
        virtual inline void insert_abort_queue(hek_action *txn);
        
 protected:
        virtual void StartWorking();
        virtual void Init();
        
 public:
        void* operator new(std::size_t sz, int cpu)
        {
                return alloc_mem(sz, cpu);
        }
        
        hek_worker(hek_worker_config conf);


};

#endif // HEK_H_
