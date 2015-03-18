#!/usr/bin/python
 
import os
import sys


fmt_locking = "build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size 1000"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size 1000"

def main():
#    small_bank_contended()
#    small_bank_uncontended()
    uncontended_1000(True, True)
#    small_bank_contended(False, True)
#    small_bank_uncontended(False, True)
#    contended_1000(False, True)

def small_bank_contended(mv, locking):
    if locking:
        os.system("rm locking.txt")
        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(2), str(0.0), str(5000000), str(100))
                os.system(cmd)
        os.system("mv locking.txt locking_small_bank_100.txt")

    if mv:
        os.system("rm results.txt")
        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:#22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(i), str(2), str(5500000), str(0.0), str(100))
                os.system(cmd)
        os.system("mv results.txt mv_small_bank_100.txt")

def small_bank_uncontended(mv, locking): 
    
    if locking:
        os.system("rm locking.txt")
        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(2), str(0.0), str(5000000), str(10000))
                os.system(cmd)
        os.system("mv locking.txt locking_small_bank_10000.txt")
    
    if mv:
        os.system("rm results.txt")
        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:#22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(i), str(2), str(5500000), str(0.0), str(10000))
                os.system(cmd)
        os.system("mv results.txt mv_small_bank_10000.txt")


def uncontended_1000(mv, locking):
    result_dir = "results/rec_1000/uncontended"
    os.system("mkdir -p " + result_dir)    
    
    if mv:
        os.system("rm results.txt")        
        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:#22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(3000000), str(1000000), str(i), str(0))
                os.system(cmd)
        outfile = os.path.join(result_dir, "mv_10rmw.txt")
        os.system("cp results.txt " + outfile)
        os.system("mv results.txt mv_10rmw.txt")

        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(3000000), str(1000000), str(i), str(1))
                os.system(cmd)
        outfile = os.path.join(result_dir, "mv_8r2rmw.txt")
        os.system("cp results.txt " + outfile)
        os.system("mv results.txt mv_8r2rm2.txt")

    if locking:
        os.system("rm locking.txt")
        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(4000000), str(1000000), str(0))
                os.system(cmd)
        outfile = os.path.join(result_dir, "locking_10rmw.txt")
        os.system("cp locking.txt " + outfile)
        os.system("mv locking.txt locking_10rmw.txt")

        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(4000000), str(1000000), str(1))
                os.system(cmd)
        outfile = os.path.join(result_dir, "locking_8r2rm2.txt")
        os.system("cp locking.txt " + outfile)
        os.system("mv locking.txt locking_8r2rmw.txt")

def contended_1000(mv, locking):

    if mv:
        os.system("rm results.txt")
        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:#22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(i), str(0), str(3000000), str(0.9), str(1000000))
                os.system(cmd)
        os.system("mv results.txt mv_contended_10rmw.txt")

        for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:#22,24,26,28,30]:
            for j in range(0, 5):
                cmd = fmt_multi.format(str(10), str(i), str(1), str(6000000), str(0.9), str(1000000))
                os.system(cmd)
        os.system("mv results.txt mv_contended_8r2rmw.txt")




    if locking:
        os.system("rm locking.txt")
        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(4000000), str(1000000), str(0))
                os.system(cmd)
        os.system("mv locking.txt locking_contended_1000_10rmw.txt")

        for i in [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40]:
            for j in range(0, 5):
                cmd = fmt_locking.format(str(i), str(4000000), str(1000000), str(1))
                os.system(cmd)
        os.system("mv locking.txt locking_contended_1000_8r2rmw.txt")

#    for i in [2,4,6,8,10,12,14,16,18,20,22,24,26,28,30]:
#        for j in range(0, 5):
#            cmd = fmt_multi.format(str(10), str(i), str(2), str(10000000), str(0.8), str(1000000))
#            os.system(cmd)
#    os.system("mv results.txt mv_small_bank_10000.txt")




    

if __name__ == "__main__":
    main()
