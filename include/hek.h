#ifndef HEK_H_
#define HEK_H_

#include <concurrent_queue.h>
#include <hek_action.h>

#define HEK_INF		0xFFFFFFFFFFFFFFF0
#define HEK_MASK	0x000000000000000F
#define EXECUTING 	0x0
#define PREPARING 	0x1
#define COMMIT		0x2
#define ABORT 		0x3

#define HEK_TIME(ts) (HEK_MASK & ts)
#define HEK_STATE(ts) ((~HEK_MASK) & ts)
#define CREATE_PREP_TIMESTAMP(ts) ((ts<<8) | PREPARING)
#define CREATE_COMMIT_TIMESTAMP(ts) ((ts<<8) | COMMIT)
#define CREATE_COMMIT_TIMESTAMP(ts) ((ts<<8) | ABORT)

class hek_queue {
        volatile hek_action *head;
        volatile hek_action **tail;

 public:
        hek_queue();
        virtual void enqueue(hek_action *txn);
        virtual hek_action* dequeue_batch();
};

struct hek_batch {
        hek_action **txns;
        uint32_t num_txns;
};

struct hek_record {
        struct hek_record *next;
        uint64_t begin;
        uint64_t end;
        uint64_t key;
        uint32_t size;
        char value[0];
};

struct hek_config {
        int cpu;
        uint64_t *global_time;
        uint32_t num_tables;
        uint32_t num_threads;
        hek_table **tables;
        SimpleQueue<hek_batch> *input_queue;
        SimpleQueue<hek_batch> *output_queue;

        hek_action **committed;
        hek_action **aborted;
};

class hek_worker : public Runnable {
 private:
        struct hek_record *records;
        
        virtual bool run_txn(hek_action *txn);
        virtual void get_reads(hek_action *txn);
        virtual struct hek_record* get_new_record();

        virtual bool validate(hek_action *txn);
        
 protected:
        virtual void StartWorking();
        virtual void Init();
        
 public:
        hek_worker(hek_config conf);


};

#endif // HEK_H_
