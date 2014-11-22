#!/usr/bin/python

import os
import sys
import os.path
import clean


fmt_locking = "build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size 1000 --distribution {4} --theta {5}"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size 1000 --distribution {5} --theta {6}"

def main():
#    small_bank_contended()
#    small_bank_uncontended()
    uncontended_1000()
#    small_bank_contended(False, True)
#    small_bank_uncontended(False, True)
#    contended_1000()
#    ccontrol()


def gen_range(low, high, diff):
    ret = []
    while low <= high:
        ret.append(low)
        low += diff
    return ret

def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, only_worker=False):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):
        os.system("rm results.txt")
        val_range = gen_range(lowThreads, highThreads, 2)

        for i in val_range:
            cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(i), str(expt), str(distribution), str(theta))
            os.system(cmd)
            os.system("cat results.txt >>" + outfile)
            clean.clean_fn("mv", outfile, temp, only_worker)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)


def locking_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):
        os.system("rm locking.txt")
        val_range = gen_range(lowThreads, highThreads, 2)
        for i in val_range:
            cmd = fmt_locking.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta))
            os.system(cmd)
            os.system("cat locking.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)
    

def ccontrol():
    outdir = "results/concurrency_control"
    for i in range(0, 5):
        mv_expt(outdir, "mv_5.txt", 5, 5000000, 1000000, 2, 20, 0, 0, 0.0, True)
        mv_expt(outdir, "mv_10.txt", 10, 5000000, 1000000, 2, 20, 0, 0, 0.0, True)
        mv_expt(outdir, "mv_15.txt", 15, 5000000, 1000000, 2, 20, 0, 0, 0.0, True)
        mv_expt(outdir, "mv_20.txt", 20, 5000000, 1000000, 2, 20, 0, 0, 0.0, True)
    
    os.system("touch " + os.path.join(outdir, "." + "mv_5.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_10.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_15.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_20.txt"))

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


def uncontended_1000():
    result_dir = "results/rec_1000/uncontended"
    for i in range(0, 5):
        mv_expt(result_dir, "mv_10rmw.txt", 10, 3000000, 1000000, 2, 30, 0, 0, 0.9)
        locking_expt(result_dir, "locking_10rmw.txt", 12, 40, 3000000, 1000000, 0, 0, 0.9)
        mv_expt(result_dir, "mv_8r2rmw.txt", 10, 3000000, 1000000, 2, 30, 1, 0, 0.9)
        locking_expt(result_dir, "locking_8r2rmw.txt", 12, 40, 3000000, 1000000, 1, 0, 0.9)

    os.system("touch " + os.path.join(outdir, "." + "mv_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_8r2rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_8r2rmw.txt"))


def contended_1000():

    result_dir = "results/rec_1000/contended"
    for i in range(0, 5):
        mv_expt(result_dir, "mv_10rmw.txt", 10, 3000000, 1000000, 2, 30, 0, 1, 0.9)
        locking_expt(result_dir, "locking_10rmw.txt", 12, 40, 3000000, 1000000, 0, 1, 0.9)
        mv_expt(result_dir, "mv_8r2rmw.txt", 10, 3000000, 1000000, 2, 30, 1, 1, 0.9)
        locking_expt(result_dir, "locking_8r2rmw.txt", 12, 40, 3000000, 1000000, 1, 1, 0.9)

    os.system("touch " + os.path.join(outdir, "." + "mv_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_8r2rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_8r2rmw.txt"))


    

if __name__ == "__main__":
    main()
