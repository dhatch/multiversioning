#ifndef YCSB_H_
#define YCSB_H_

#include <db.h>
#include <vector>

using namespace std;

class ycsb_rmw : public txn {
 private:
        vector<uint64_t> reads;
        vector<uint64_t> writes;
        
 public:
        ycsb_rmw(vector<uint64_t> reads, vector<uint64_t> writes);
        virtual bool Run();
};

#endif // YCSB_H_
