#!/usr/bin/python

import os
import sys
import os.path
import clean


fmt_locking = "numactl --interleave=all build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --read_pct {7} --read_txn_size 10000"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct {8} --read_txn_size 10000"

fmt_occ = "numactl --interleave=all build/db --cc_type 2  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 10000"

fmt_hek = "build/db --cc_type 3  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 10000"

fmt_si = "build/si --cc_type 3  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct {7} --read_txn_size 5"

fmt_multi_cc = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size {8} --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct 0 --read_txn_size 10"


def main():
#    write_searches_top()
#    small_bank_uncontended()
#    small_bank_contended()
#    small_bank_reads()
#    uncontended_1000()
#    small_bank_contended(False, True)
#    small_bank_uncontended(False, True)
#    contended_1000()
#    ccontrol()
#    vary_contention()

#    search_best()
#    test_cc()
#    exp_0()
#    occ_uncontended_1000()
    test_locking()
#    search_best()
#    test_cc()
#    ccontrol()
    
#    combine_best()
#    small_bank_contended()
#    small_bank_uncontended()
#    write_contended()
#    write_uncontended()
#    exp_1()
#    exp_2()

def test_locking():
    low_dir = "results/low_contention/"
    high_dir = "results/high_contention/"
    p10rmw = "10rmw"
    p2rmw8r = "2rmw8r"
    for i in range(0, 4):
        cur_dir = os.path.join(low_dir, p2rmw8r)
        occ_expt(cur_dir, "occ.txt", 4, 40, 3000000, 1000000, 1, 1, 0.0, 1000, 0)
        locking_expt(cur_dir, "locking.txt", 4, 40, 3000000, 1000000, 1, 1, 0.0, 1000, 0)

        cur_dir = os.path.join(high_dir, p10rmw)
        occ_expt(cur_dir, "occ.txt", 4, 40, 3000000, 1000000, 0, 1, 0.9, 1000, 0)
        locking_expt(cur_dir, "locking.txt", 4, 40, 3000000, 1000000, 0, 1, 0.9, 1000, 0)

        cur_dir = os.path.join(high_dir, p2rmw8r)
        occ_expt(cur_dir, "occ.txt", 4, 40, 3000000, 1000000, 1, 1, 0.9, 1000, 0)
        locking_expt(cur_dir, "locking.txt", 4, 40, 3000000, 1000000, 1, 1, 0.9, 1000, 0)

        cur_dir = os.path.join(low_dir, p10rmw)
        occ_expt(cur_dir, "occ.txt", 4, 40, 3000000, 1000000, 0, 1, 0.0, 1000, 0)
        locking_expt(cur_dir, "locking.txt", 4, 40, 3000000, 1000000, 0, 1, 0.0, 1000, 0)



def print_cc():
    clean.cc_fn("results.txt", "cc_out.txt")

def single_cc_test(txn_size):
    os.system("rm results.txt")
    thread_range = [4,8,12,16,20]
    for t in thread_range:
        if t < 10:
            num_txns = 200000
        else:
            num_txns = 1000000
        cmd = fmt_multi_cc.format(str(t), str(num_txns), str(1000000), str(1),
                                  str(0), str(0), str(0.0), str(1000), str(txn_size))
        os.system(cmd)
    os.system("cat results.txt >>  cc_" + str(txn_size) + ".txt")


def test_cc():
    os.system("rm results.txt")
    for i in range(0, 10):
        single_cc_test(10)
        single_cc_test(5)
        single_cc_test(1)
    
        

def gen_range(low, high, diff):
    ret = []
    while low <= high:
        ret.append(low)
        low += diff
    return ret


def mv_expt_theta(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p " + outdir)
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        os.system(cmd)
        os.system("cat results.txt >>" + outfile)
        clean.theta_fn("mv", outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        if expt == 0:
            os.system("gnuplot plot.plt")
        else:
            os.system("gnuplot plot1.plt")
        os.chdir(saved_dir)

def mv_expt_theta(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        os.system(cmd)
        os.system("cat results.txt >>" + outfile)
        clean.theta_fn("mv", outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        if expt == 0:
            os.system("gnuplot plot.plt")
        else:
            os.system("gnuplot plot1.plt")
        os.chdir(saved_dir)


def mv_expt_records(outdir, filename, ccThreads, txns, records, threads, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):
        os.system("rm results.txt")

        cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(threads), str(expt), str(distribution), str(theta), str(rec_size))
        
        subprocess.call(cmd)

        os.system("cat results.txt >>" + outfile)
        clean.records_fn(outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        os.system("gnuplot plot.plt")
        os.chdir(saved_dir)


def locking_expt_records(outdir, filename, threads, txns, records, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):
        os.system("rm locking.txt")

        cmd = fmt_locking.format(str(threads), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size))
        os.system(cmd)
        os.system("cat locking.txt >>" + outfile)
        clean.records_fn(outfile, temp)
        saved_dir = os.getcwd()
        os.chdir(outdir)
        os.system("gnuplot plot.plt")
        os.chdir(saved_dir)


def mv_expt_single(outdir, filename, ccThreads, txns, records, workers, expt, 
                   distribution, theta, rec_size, only_worker=False):
    outfile = os.path.join(outdir, filename)
    os.system("mkdir -p " + outdir)
    os.system("rm results.txt")
    cmd = fmt_multi.format(str(ccThreads), str(txns), str(records),
                           str(workers), str(expt), str(distribution),
                           str(theta), str(rec_size), str(0))
    os.system(cmd)
    os.system("cat results.txt >>" + outfile)
    saved_dir = os.getcwd()
    os.chdir(outdir)
    os.chdir(saved_dir)

def mv_single(outdir, filename, ccThreads, txns, records, worker_threads, expt,
              distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    os.system("mkdir -p " + outdir)
    os.system("rm results.txt")
    cmd = fmt_multi.format(str(ccThreads), str(txns), str(records),
                           str(worker_threads), str(expt), str(distribution),
                           str(theta), str(rec_size))
    os.system(cmd)
    os.system("cat results.txt >>" + outfile)
    saved_dir = os.getcwd()
    os.chdir(outdir)
    os.chdir(saved_dir)
    
        
def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, rec_size, pct, only_worker=False):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)

        for i in val_range:
            os.system("rm results.txt")
            cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(i), str(expt), str(distribution), str(theta), str(rec_size), str(pct))
            os.system(cmd)
            os.system("cat results.txt >>" + outfile)
            clean.clean_fn("mv", outfile, temp, only_worker)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)


def locking_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm locking.txt")
            cmd = fmt_locking.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct))
            os.system(cmd)
            os.system("cat locking.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)

def occ_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    
    os.system("mkdir -p " + outdir)
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm occ.txt")
            cmd = fmt_occ.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct))
            os.system(cmd)
            os.system("cat occ.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)



def si_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")


#     param_dict = {}
#     param_dict["--cc_type"] = str(3)
#     param_dict["--num_contended"] = str(2)
#     param_dict["--txn_size"] = str(10)
#     param_dict["--read_pct"] = str(0)
#     param_dict["--read_txn_size"] = str(5)
# 
#     param_dict["--num_txns"] = str(txns)
#     param_dict["--num_records"] = str(records)
#     param_dict["--experiment"] = str(expt)
#     param_dict["--distribution"] = str(distribution)
#     param_dict["--record_size"] = str(1000)
#     
#     os.system("mkdir -p " + outdir)
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm hek.txt")
            cmd = fmt_si.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size))
            os.system(cmd)
            os.system("cat hek.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)


def hek_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size, read_pct):
    outfile = os.path.join(outdir, filename)
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")


#     param_dict = {}
#     param_dict["--cc_type"] = str(3)
#     param_dict["--num_contended"] = str(2)
#     param_dict["--txn_size"] = str(10)
#     param_dict["--read_pct"] = str(0)
#     param_dict["--read_txn_size"] = str(5)
# 
#     param_dict["--num_txns"] = str(txns)
#     param_dict["--num_records"] = str(records)
#     param_dict["--experiment"] = str(expt)
#     param_dict["--distribution"] = str(distribution)
#     param_dict["--record_size"] = str(1000)
#     
#     os.system("mkdir -p " + outdir)
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm hek.txt")
            cmd = fmt_hek.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size), str(read_pct))
            os.system(cmd)
            os.system("cat hek.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)

            
def ccontrol():
    outdir = "results/hekaton/concurrency_control"
    for i in range(0, 5):
        mv_expt(outdir, "mv_5.txt", 5, 5000000, 1000000, 2, 20, 0, 0, 0.0, 1000, True)
        mv_expt(outdir, "mv_10.txt", 10, 5000000, 1000000, 2, 20, 0, 0, 0.0, 1000, True)
        mv_expt(outdir, "mv_15.txt", 15, 5000000, 1000000, 2, 20, 0, 0, 0.0, 1000, True)
        mv_expt(outdir, "mv_20.txt", 20, 5000000, 1000000, 2, 20, 0, 0, 0.0, 1000, True)
    
#    os.system("touch " + os.path.join(outdir, "." + "mv_5.txt"))
#    os.system("touch " + os.path.join(outdir, "." + "mv_10.txt"))
#    os.system("touch " + os.path.join(outdir, "." + "mv_15.txt"))
#    os.system("touch " + os.path.join(outdir, "." + "mv_20.txt"))



def small_bank_reads():
    result_dir = "results/small_bank/contended/varying/reads"
    for j in range(0, 5):
        for i in [10,50,100,500,1000,5000,10000,50000,100000]:
            mv_expt_records(result_dir, "mv.txt", 10, 10000000, i, 30, 2, 0, 0.9, 8)
            locking_expt_records(result_dir, "locking.txt", 40, 10000000, i, 2, 0, 0.9, 8)

    os.system("touch " + os.path.join(result_dir, "." + "mv.txt"))
    os.system("touch " + os.path.join(result_dir, "." + "locking.txt"))
    

def small_bank_uncontended(): 
    result_dir = "results/small_bank_new/uncontended/"
    occ_expt(result_dir, "occ_uncontended.txt", 2, 40, 10000000, 100000, 2, 0, 0.9, 1000)
#    for i in range(0, 5):
#        mv_expt(result_dir, "mv_uncontended.txt", 10, 10000000, 100000, 2, 30, 2, 0, 0.9, 1000)

#    os.system("touch " + os.path.join(result_dir, "." + "mv_uncontended.txt"))
#    os.system("touch " + os.path.join(result_dir, "." + "locking_uncontended.txt"))


def small_bank_contended(): 
    result_dir = "results/small_bank_new/contended/"
    occ_expt(result_dir, "occ_contended.txt", 2, 40, 10000000, 100, 2, 0, 0.9, 1000)
#    for i in range(0, 5):
#        mv_expt(result_dir, "mv_contended.txt", 10, 10000000, 100, 2, 30, 2, 0, 0.9, 1000)

#    os.system("touch " + os.path.join(result_dir, "." + "mv_contended.txt"))
#    os.system("touch " + os.path.join(result_dir, "." + "locking_contended.txt"))



def vary_contention():
    result_dir = "results/vary_contention"
    for i in range(0, 5):
        for j in [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]:
            mv_expt_theta(result_dir, "mv_10rmw.txt", 14, 3000000, 1000000, 26, 0, 1, j, 1000)
            locking_expt_theta(result_dir, "locking_10rmw.txt", 40, 3000000, 1000000, 0, 1, j, 1000)
            mv_expt_theta(result_dir, "mv_8r2rmw.txt", 14, 3000000, 1000000, 26, 1, 1, j, 1000)
            locking_expt_theta(result_dir, "locking_8r2rmw.txt", 40, 3000000, 1000000, 1, 1, j, 1000)


def uncontended_1000():
    result_dir = "results/rec_1000/uncontended"
    for i in range(0, 5):
        mv_expt(result_dir, "mv_10rmw.txt", 14, 3000000, 1000000, 2, 26, 0, 0, 0.9, 1000)
        locking_expt(result_dir, "locking_10rmw.txt", 12, 40, 3000000, 1000000, 0, 0, 0.9, 1000)
        mv_expt(result_dir, "mv_8r2rmw.txt", 14, 3000000, 1000000, 2, 26, 1, 0, 0.9, 1000)
        locking_expt(result_dir, "locking_8r2rmw.txt", 12, 40, 3000000, 1000000, 1, 0, 0.9, 1000)

    os.system("touch " + os.path.join(outdir, "." + "mv_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_8r2rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_8r2rmw.txt"))

def hek_uncontended_1000():

     for i in range (0, 20):
#         result_dir = "results/hekaton/small_bank/uncontended/"
#         si_expt(result_dir, "si_small_bank.txt", 4, 40, 1000000, 100000, 3, 1, 0.0, 1000)
#         hek_expt(result_dir, "hek_small_bank.txt", 4, 40, 1000000, 100000, 3, 1, 0.0, 1000)
#         occ_expt(result_dir, "occ_small_bank.txt", 4, 40, 1000000, 100000, 3, 1, 0.0, 1000)
#         mv_expt(result_dir, "mv_small_bank.txt", 4, 1000000, 100000, 4, 36, 3, 1, 0.0, 1000)
# 
         result_dir = "results/hekaton/small_bank/contended/"
         locking_expt(result_dir, "pess_small_bank.txt", 24, 32, 3000000, 50, 2, 1, 0.0, 1000, 0.0)
#         si_expt(result_dir, "si_small_bank.txt", 4, 40, 1000000, 25, 3, 1, 0.0, 1000)
#         hek_expt(result_dir, "hek_small_bank.txt", 4, 40, 1000000, 25, 3, 1, 0.0, 1000)
#         occ_expt(result_dir, "occ_small_bank.txt", 4, 40, 1000000, 25, 3, 1, 0.0, 1000)
#         mv_expt(result_dir, "mv_small_bank.txt", 4, 1000000, 25, 4, 36, 3, 1, 0.0, 1000)
# 

#    theta_range = [100000,50000,10000,5000,1000,500,100,50,10,5]

#     result_dir = "results/hekaton/small_bank/varying"
#     read_pct_range = [50]
#     num_readonly = 1000
# 
#     for i in range(0, 5):
#         result_dir = "results/hekaton/ycsb/contended/10rmw/"
#         occ_expt(result_dir, "silo_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000, 0.0)
#         locking_expt(result_dir, "pess_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000, 0.0)
# 
#         result_dir = "results/hekaton/ycsb/contended/8r2rmw/"
#         occ_expt(result_dir, "silo_8r2rmw.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000, 0.0)
#         locking_expt(result_dir, "pess_8r2rmw.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000, 0.0)
# 
#         result_dir = "results/hekaton/ycsb/uncontended/10rmw/"
#         occ_expt(result_dir, "silo_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.0, 1000, 0.0)
#         locking_expt(result_dir, "pess_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.0, 1000, 0.0)
# 
#         result_dir = "results/hekaton/ycsb/uncontended/8r2rmw/"
#         occ_expt(result_dir, "silo_8r2rmw.txt", 4, 40, 1000000, 1000000, 1, 1, 0.0, 1000, 0.0)
#         locking_expt(result_dir, "pess_8r2rmw.txt", 4, 40, 1000000, 1000000, 1, 1, 0.0, 1000, 0.0)
# 
#         
        


#         for theta in theta_range:
#             si_expt(result_dir, "si.txt", 40, 40, 3000000, theta, 3, 1, 0.0, 1000)
#             hek_expt(result_dir, "hek.txt", 40, 40, 3000000, theta, 3, 1, 0.0, 1000)
#             occ_expt(result_dir, "occ.txt", 40, 40, 3000000, theta, 3, 1, 0.0, 1000)



#        si_expt(result_dir, "si_small_bank.txt", 4, 40, 1000000, 100, 3, 1, 0.0, 1000)

#        si_expt(result_dir, "si_small_bank.txt", 24, 40, 5000000, 1000000, 3, 1, 0.0, 1000)
#        hek_expt(result_dir, "hek_small_bank.txt", 24, 40, 5000000, 1000000, 3, 1, 0.0, 1000)


#         locking_expt(result_dir, "locking_8r2rmw.txt", 4, 40, 3000000, 1000000, 1, 1, 0.9, 1000)
# 
#          result_dir = "results/hekaton/ycsb/uncontended/10rmw_scan/"
#          for pct in read_pct_range:
#              num_txns = int(100.0 * float(1000.0) / float(pct))
# 
#  
#              if pct == 1:
#                  hek_expt(result_dir, "hek_10rmw.txt", 40, 40, 100000, 1000000, 0, 1, 0.0, 1000, pct)
# #                  occ_expt(result_dir, "occ_10rmw.txt", 40, 40, 100000, 1000000, 0, 1, 0.0, 1000, pct)
# #                  mv_expt(result_dir, "mv_10rmw.txt", 14, 100000, 1000000, 26, 26, 0, 1, 0.0, 1000, pct)
#              else:
#                  hek_expt(result_dir, "hek_10rmw.txt", 40, 40, 10000, 1000000, 0, 1, 0.0, 1000, pct)
# #                  occ_expt(result_dir, "occ_10rmw.txt", 40, 40, 10000, 1000000, 0, 1, 0.0, 1000, pct)
# #                  mv_expt(result_dir, "mv_10rmw.txt", 14, 10000, 1000000, 26, 26, 0, 1, 0.0, 1000, pct)
#                  
# #             locking_expt(result_dir, "pess_10rmw.txt", 40, 40, num_txns, 1000000, 0, 1, 0.0, 1000, pct)
# # 
#     
#		for i in range(0, 10):
#        result_dir = "results/hekaton/ycsb/uncontended/8r2rmw/"
#        si_expt(result_dir, "si_8r2rmw.txt", 24, 36, 3000000, 1000000, 1, 1, 0.0, 1000)
#        hek_expt(result_dir, "hek_8r2rmw.txt", 20, 40, 5000000, 1000000, 1, 1, 0.0, 1000)
#        occ_expt(result_dir, "occ_8r2rmw.txt", 40, 40, 1000000, 1000000, 1, 1, 0.0, 1000)
#        locking_expt(result_dir, "locking_8r2rmw.txt", 4, 40, 3000000, 1000000, 1, 1, 0.0, 1000)

#        result_dir = "results/hekaton/ycsb/uncontended/10rmw/"
#        hek_expt(result_dir, "hek_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.0, 1000)
#        si_expt(result_dir, "si_10rmw.txt", 20, 36, 1000000, 1000000, 0, 1, 0.0, 1000)
#        occ_expt(result_dir, "occ_10rmw.txt", 20, 40, 1000000, 1000000, 0, 1, 0.0, 1000)
#        locking_expt(result_dir, "locking_10rmw.txt", 4, 40, 3000000, 1000000, 0, 1, 0.0, 1000)






#    for i in range(0, 5):


#        occ_expt(result_dir, "occ_10rmw.txt", 4, 40, 1000000, 1000, 0, 1, 0.0, 1000)
#        occ_expt(result_dir, "occ_8r2rmw.txt", 4, 40, 1000000, 1000, 1, 1, 0.0, 1000)

        
#        hek_expt(result_dir, "hek_8r2rmw.txt", 4, 40, 3000000, 1000, 1, 1, 0.0, 1000)
#        hek_expt(result_dir, "hek_10rmw.txt", 4, 40, 1000000, 1000, 0, 1, 0.0, 1000)
#        si_expt(result_dir, "si_10rmw.txt", 4, 40, 1000000, 1000, 0, 1, 0.0, 1000)
#        si_expt(result_dir, "si_8r2rmw.txt", 4, 40, 3000000, 1000, 1, 1, 0.0, 1000)

#        occ_expt(result_dir, "occ_10rmw.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000)
#        occ_expt(result_dir, "occ_8r2rmw.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv_10rmw.txt", 10, 1000000, 1000000, 2, 30, 0, 1, 0.0, 1000)
#        mv_expt(result_dir, "mv_8r2rmw.txt", 10, 1000000, 500, 2, 30, 1, 1, 0.0, 1000)
        
    
    
def occ_uncontended_1000():
    result_dir = "results/rec_1000/uncontended"
    for i in range(0, 5):
        occ_expt(result_dir, "occ_8r2rmw.txt", 12, 40, 10000000, 1000000, 1, 0, 0.9, 1000)
        occ_expt(result_dir, "occ_8r2rmw.txt", 4, 8, 1000000, 1000000, 1, 0, 0.9, 1000)
        occ_expt(result_dir, "occ_10rmw.txt", 4, 8, 1000000, 1000000, 0, 0, 0.9, 1000)
        occ_expt(result_dir, "occ_10rmw.txt", 12, 40, 10000000, 1000000, 0, 0, 0.9, 1000)

def occ_contended_1000():
    result_dir = "results/rec_1000/contended"
    for i in range(0, 5):
        occ_expt(result_dir, "occ_8r2rmw.txt", 12, 40, 10000000, 1000000, 1, 1, 0.9, 1000)
        occ_expt(result_dir, "occ_8r2rmw.txt", 4, 8, 1000000, 1000000, 1, 1, 0.9, 1000)
        occ_expt(result_dir, "occ_10rmw.txt", 4, 8, 1000000, 1000000, 0, 1, 0.9, 1000)
        occ_expt(result_dir, "occ_10rmw.txt", 12, 40, 10000000, 1000000, 0, 1, 0.9, 1000)
        
def write_contended():
    result_dir = "results/rec_1000/write_only"
#    locking_expt(result_dir, "locking_2r8w.txt", 4, 40, 3000000, 1000000, 1, 1, 0.9, 1000)
    occ_expt(result_dir, "occ_2r8w.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000)

def check_worse(input_file):
    if os.path.exists(input_file):
        times = clean.list_times(input_file)
        if len(times) > 2:
            l0 = times[len(times)-1]["time"]
            l1 = times[len(times)-2]["time"]
            l2 = times[len(times)-3]["time"]
            if l0 > l1 and l1 > l2:
                return True
            else:
                return False
    return False
        
def compute_best_conf(input_file):
    times = clean.list_times(input_file)
    min_time = times[0]
    for val in times:
        if val["time"] < min_time["time"]:
            min_time = val
    return min_time

def get_best(input_files):
    ret = []
    for f in input_files:
        temp = compute_best_conf(f)
        ret.append(temp["time"])
    return ret

def write_mv_output(input_list, output_file):
    out = open(output_file, 'w')
    for val in input_list:
        out_line = str(val["threads"]) + " " + str(1000/val["time"]) + " " + str(1000000/val["time"]) + " " + str(1000000/val["time"]) + "\n"
        out.write(out_line)
    out.close()

def combine_best():
    result_dir = "results/final/search/0_9/temp"
    core_list = [4,8,12,16,20,24,28,32,36,40]
    files = []
    for i in [4,8,12,16,20,24,28,32,36,40]:
        files.append(os.path.join(result_dir, str(i)+".txt"))

    times = get_best(files)
    vals = []
    i = 0
    for t in times:
        cur = {}
        cur["time"] = t
        cur["threads"] = core_list[i]
        vals.append(cur)
        i += 1
    print vals
    write_mv_output(vals, "mv_out.txt")
        
def search_best_inner():
    result_dir = "results/final/search/2_9/"
    prev_best = 1
    for i in [4,8,12,16,20,24,28,32,36,40]:
        if prev_best > 1:
            cc_threads = prev_best-1
        else:
            cc_threads = prev_best
        while cc_threads < i and not(check_worse(os.path.join(result_dir, str(i)+".txt"))):
            worker_threads = i - cc_threads
            mv_single(result_dir, str(i)+".txt", cc_threads,
                      1000000, 1000000, worker_threads, 2, 1, 0.9, 1000)
            cc_threads += 1
        prev_best = compute_best_conf(os.path.join(result_dir, str(i)+".txt"))["threads"]


def exp_0():
    top_level = "results/final/txn_size_10"

    for i in range(0, 5):
        result_dir = os.path.join(top_level, "0_0/")
        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 0, 1, 0.0, 1000)
        result_dir = os.path.join(top_level, "0_9/")
        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000)
        


#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 0, 1, 0.0, 1000)
#
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/0_1/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 1, 1, 0.0, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 1, 1, 0.0, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/0_2/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 2, 1, 0.0, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 2, 1, 0.0, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/9_0/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 0, 1, 0.9, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/9_1/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 1, 1, 0.9, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/9_2/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 2, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 2, 1, 0.9, 1000)


def search_best():
    for i in range(0, 5):
        search_best_inner()
            
def exp_0():

#    result_dir = "results/final/ycsb/temp2/"
#    occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 0, 1, 0.0, 1000)
#    mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 0, 1, 0.0, 1000)
#
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/0_1/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 1, 1, 0.0, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 1, 1, 0.0, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/0_2/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 2, 1, 0.0, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 2, 1, 0.0, 1000)
#    for i in range(0, 5):
#    result_dir = "results/final/ycsb/9_temp/"
#    occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 0, 1, 0.9, 1000)
#    mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 0, 1, 0.9, 1000)
#    for i in range(0, 5):
#        result_dir = "results/final/ycsb/9_1/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 1, 1, 0.9, 1000)
#    for i in range(0, 5):
        result_dir = "results/final/ycsb/9_temp_2/"
#        occ_expt(result_dir, "occ.txt", 4, 40, 1000000, 1000000, 2, 1, 0.9, 1000)
        mv_expt(result_dir, "mv.txt", 10, 1000000, 1000000, 2, 30, 2, 1, 0.9, 1000)
#
#
    

def write_uncontended():
    result_dir = "results/rec_1000/writes_uncontended"
    occ_expt(result_dir, "occ_2r8w.txt", 4, 40, 1000000, 1000000, 1, 1, 0.0, 1000)
    mv_expt(result_dir, "mv_2r8w.txt", 10, 1000000, 1000000, 2, 30, 1, 1, 0.0, 1000)


def contended_1000():

    result_dir = "results/rec_1000/contended"
    for i in range(0, 5):
#        mv_expt(result_dir, "mv_10rmw.txt", 14, 3000000, 1000000, 2, 26, 0, 1, 0.9, 1000)
        locking_expt(result_dir, "locking_10rmw.txt", 2, 10, 3000000, 1000000, 0, 1, 0.9, 1000)
#        mv_expt(result_dir, "mv_8r2rmw.txt", 14, 3000000, 1000000, 2, 26, 1, 1, 0.9, 1000)
        locking_expt(result_dir, "locking_8r2rmw.txt", 2, 10, 3000000, 1000000, 1, 1, 0.9, 1000)

    os.system("touch " + os.path.join(outdir, "." + "mv_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_8r2rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_10rmw.txt"))
    os.system("touch " + os.path.join(outdir, "." + "locking_8r2rmw.txt"))


def get_fastest(outfile):
    times = clean.list_times(outfile)
    max_thpt = times[0]
    
    for val in times:
        cur_throughput = float(val["txns"])/float(val["time"])
        max_throughput = float(max_thpt["txns"])/float(max_thpt["time"])
        if cur_throughput > max_throughput:
            max_thpt = val
    print max_thpt
    return max_thpt

def transform_mv_result(result_list):
    result_list.sort()
    median_val = result_list[len(result_list)/2]
    min_val = result_list[0]
    max_val = result_list[len(result_list)-1]
    return [median_val, min_val, max_val]

def write_search_inner(outfile, median_dict):
    f = open(outfile, 'w')
    line = "%(threads)d %(median)f %(min)f %(max)f\n"
    keys = list(median_dict.keys())
    keys.sort()
    for thread in keys:
        median_list = median_dict[thread]
        outline = line % {'threads':thread, 'median':median_list[0],
                          'min':median_list[1], 'max':median_list[2]}
        f.write(outline)
    f.close()

def write_search_results(outfile, result_files):
    result_dict = {}
    for result in result_files:
        threads = result["threads"]
        result_file = result["file"]
        print result
        if not os.path.exists(result_file):
            continue
        fastest = get_fastest(result_file)
        fastest_throughput = float(fastest["txns"])/float(fastest["time"])
        if not threads in result_dict:
            result_dict[threads] = []
        result_dict[threads].append(fastest_throughput)
        
    print result_dict
    median_dict = {}
    for thread in result_dict:
        median_dict[thread] = clean.confidence_interval(result_dict[thread])
    write_search_inner(outfile, median_dict)

    
def create_file_dict(result_dir, threads):
    ret = []
    for t in threads:
        cur = {}
        cur["threads"] = t
        filename = str(t) + ".txt"
        cur["file"] = os.path.join(result_dir, filename)
        ret.append(cur)
    return ret

def write_mv_searches(outfile, toplevel, dirs):
    num_threads = [4,8,12,16,20,24,28,32,36,40]
    file_dict = []
    for t in num_threads:
        leaf_name = str(t) + ".txt"
        for d in dirs:
            dirname = os.path.join(toplevel, d)
            fname = os.path.join(dirname, leaf_name)
            entry = {}
            entry["threads"] = t
            entry["file"] = fname
            file_dict.append(entry)
    write_search_results(outfile, file_dict)

def write_mv_searches_theta(outfile, toplevel, dirs):
    theta_list = [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]
    results = {}
    confidences = {}
    for t in theta_list:
        leaf_name = str(int(10*t)) + ".txt"
        for d in dirs:
            dirname = os.path.join(toplevel, d)
            fname = os.path.join(dirname, leaf_name)
            val = get_fastest(fname)
            throughput = float(val["txns"]) / val["time"]
            if not t in results:
                results[t] = []
            results[t].append(throughput)
            
    print results
    for key in results:
        confidences[key] = clean.confidence_interval(results[key])
    print confidences
    write_search_inner(outfile, confidences)
        

def write_searches_top(result_dirs):
#    result_dir = "results/hekaton/ycsb/bohm/0/"
    threads = [4,8,12,16,20,24,28,32,36,40]
    file_dict = create_file_dict(result_dir, threads)
    write_search_results("mv_out.txt", file_dict)
    
def check_best(outfile):
    temp = get_fastest(outfile)
    return temp["threads"]



def check_increasing(outfile):
    if os.path.exists(outfile):
        times = clean.list_times(outfile)
        num_entries = len(times)
        if len(times) > 3:
            t0 = times[num_entries-1]["time"]
            t1 = times[num_entries-2]["time"]
            t2 = times[num_entries-3]["time"]
            return t0 > t1 and t1 > t2 
    return False
    
    

def search_best_inner(expt, theta, num_records, out_dir):    
    thread_range = [4,8,12,16,20,24,28,32,36,40]
    prev_best = 0
    for t in thread_range:
        filename = str(t) + ".txt"
        outfile = os.path.join(out_dir, filename)
        if prev_best > 3:
            ccthreads = prev_best-2
        else:
            ccthreads = 2
        while ccthreads < t and not check_increasing(outfile):
            worker_threads = t-ccthreads
#            if ccthreads < 4:
#                num_txns = 300000
#            elif ccthreads < 9:
#                num_txns = 500000
#            else:
#                num_txns = 1000000
            num_txns = 3000000
            mv_expt_single(out_dir, filename, ccthreads, num_txns, num_records,
                           worker_threads, expt, 1, theta, 1000)

            ccthreads += 1
        prev_best = check_best(outfile)

#    top_level = "results/final/txn_size_10"

def search_best_theta(expt, out_dir):
    theta_range = [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
    cc_range1 = range(8, 20)
    cc_range2 = range(8, 11)
    for t in theta_range:
        filename = str(int(10 * t)) + ".txt"
        outfile = os.path.join(out_dir, filename)
        total_threads = 40
        if t == 0.9:
            cc_r = cc_range2
        else:
            cc_r = cc_range1
        for num_cc in cc_r:
            num_execs = total_threads - num_cc
            mv_expt_single(out_dir, filename, num_cc, 1000000, 1000000, 
                           num_execs, expt, 1, t, 1000)
            
def search_best():
    for i in range(1, 5):
        high_contention = "results/hekaton/small_bank/contended/bohm/"
        temp = os.path.join(high_contention, str(i))
        search_best_inner(3, 0.0, 50, temp)
 
        low_contention = "results/hekaton/small_bank/uncontended/bohm/"
        temp = os.path.join(low_contention, str(i))
        search_best_inner(3, 0.0, 100000, temp)
# 
#        low_contention = "results/hekaton/ycsb/uncontended/8r2rmw/bohm"
#        temp = os.path.join(low_contention, str(i))
#        search_best_inner(1, 0.0, 1000000, temp)
# 
 

#        temp = os.path.join(high_contention, str(i))
#        search_best_inner(0, 0.9, temp)


if __name__ == "__main__":
    main()
