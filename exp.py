#!/usr/bin/python

import os
import sys
import os.path
import clean


fmt_locking = "build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5}"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size {7} --distribution {5} --theta {6}"

fmt_occ = "build/db --cc_type 2  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000"

def main():
#    small_bank_uncontended()
#    small_bank_contended()
#    small_bank_reads()
#    uncontended_1000()
#    small_bank_contended(False, True)
#    small_bank_uncontended(False, True)
#    contended_1000()
#    ccontrol()
#    vary_contention()

#    occ_uncontended_1000()
    small_bank_contended()
    small_bank_uncontended()

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
        os.system(cmd)
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



def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, rec_size, only_worker=False):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 2)

        for i in val_range:
            os.system("rm results.txt")
            cmd = fmt_multi.format(str(ccThreads), str(txns), str(records), str(i), str(expt), str(distribution), str(theta), str(rec_size))
            os.system(cmd)
            os.system("cat results.txt >>" + outfile)
            clean.clean_fn("mv", outfile, temp, only_worker)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)


def locking_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 2)
        for i in val_range:
            os.system("rm locking.txt")
            cmd = fmt_locking.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size))
            os.system(cmd)
            os.system("cat locking.txt >>" + outfile)
            clean.clean_fn("locking", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)

def occ_expt(outdir, filename, lowThreads, highThreads, txns, records, expt, distribution, theta, rec_size):
    outfile = os.path.join(outdir, filename)
    
    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    outdep = os.path.join(outdir, "." + filename)
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)
        for i in val_range:
            os.system("rm occ.txt")
            cmd = fmt_occ.format(str(i), str(txns), str(records), str(expt), str(distribution), str(theta), str(rec_size))
            os.system(cmd)
            os.system("cat occ.txt >>" + outfile)
            clean.clean_fn("occ", outfile, temp)
            saved_dir = os.getcwd()
            os.chdir(outdir)
            os.system("gnuplot plot.plt")
            os.chdir(saved_dir)
            
def ccontrol():
    outdir = "results/concurrency_control"
    for i in range(0, 5):
        mv_expt(outdir, "mv_5.txt", 5, 5000000, 1000000, 2, 20, 0, 0, 0.0, 8, True)
        mv_expt(outdir, "mv_10.txt", 10, 5000000, 1000000, 2, 20, 0, 0, 0.0, 8, True)
        mv_expt(outdir, "mv_15.txt", 15, 5000000, 1000000, 2, 20, 0, 0, 0.0, 8, True)
        mv_expt(outdir, "mv_20.txt", 20, 5000000, 1000000, 2, 20, 0, 0, 0.0, 8, True)
    
    os.system("touch " + os.path.join(outdir, "." + "mv_5.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_10.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_15.txt"))
    os.system("touch " + os.path.join(outdir, "." + "mv_20.txt"))



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


    

if __name__ == "__main__":
    main()
