#!/usr/bin/python

import os
import sys
import os.path
import clean


fmt_locking = "build/db --cc_type 1  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --read_pct 0 --read_txn_size 10"

fmt_multi = "build/db --cc_type 0 --num_cc_threads {0} --num_txns {1} --epoch_size 10000 --num_records {2} --num_worker_threads {3} --txn_size 10 --experiment {4} --record_size {7} --distribution {5} --theta {6} --read_pct 0 --read_txn_size 10"

fmt_occ = "build/db --cc_type 2  --num_lock_threads {0} --num_txns {1} --num_records {2} --num_contended 2 --txn_size 10 --experiment {3} --record_size {6} --distribution {4} --theta {5} --occ_epoch 8000000 --read_pct 0 --read_txn_size 5"

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
    test_cc()
#    exp_0()
#    occ_uncontended_1000()
#    small_bank_contended()
#    small_bank_uncontended()
#    write_contended()
#    write_uncontended()
#    exp_1()
#    exp_2()

def print_cc():
    clean.cc_fn("results.txt", "cc_out.txt")

def test_cc():
#    os.system("rm results.txt")
    for i in range(0, 3):
        thread_range = [2,4,6,8,10,12,14,16,18,20]
        for t in thread_range:
            if t < 10:
                num_txns = 500000
            else:
                num_txns = 1000000
            cmd = fmt_multi.format(str(t), str(num_txns), str(1000000), str(1),
                                   str(0), str(0), str(0.0), str(1000))
            os.system(cmd)
    
        

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


def mv_expt_single(outdir, filename, ccThreads, txns, records, workers, expt, 
                   distribution, theta, rec_size, only_worker=False):
    outfile = os.path.join(outdir, filename)
    os.system("mkdir -p " + outdir)
    os.system("rm results.txt")
    cmd = fmt_multi.format(str(ccThreads), str(txns), str(records),
                           str(workers), str(expt), str(distribution),
                           str(theta), str(rec_size))
    os.system(cmd)
    os.system("cat results.txt >>" + outfile)
    saved_dir = os.getcwd()
    os.chdir(outdir)
    os.chdir(saved_dir)

def mv_expt(outdir, filename, ccThreads, txns, records, lowThreads, highThreads, expt, distribution, theta, rec_size, only_worker=False):
    outfile = os.path.join(outdir, filename)
    outdep = os.path.join(outdir, "." + filename)

    temp = os.path.join(outdir, filename[:filename.find(".txt")] + "_out.txt")

    os.system("mkdir -p outdir")
    if not os.path.exists(outdep):

        val_range = gen_range(lowThreads, highThreads, 4)

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

        val_range = gen_range(lowThreads, highThreads, 4)
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

    os.system("mkdir -p " + outdir)
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
        
def write_contended():
    result_dir = "results/rec_1000/write_only"
#    locking_expt(result_dir, "locking_2r8w.txt", 4, 40, 3000000, 1000000, 1, 1, 0.9, 1000)
    occ_expt(result_dir, "occ_2r8w.txt", 4, 40, 1000000, 1000000, 1, 1, 0.9, 1000)


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
        fastest_throughput = float(fastest["txns"])/float(fastest["time"]*1000)
        if not threads in result_dict:
            result_dict[threads] = []
        result_dict[threads].append(fastest_throughput)

    median_dict = {}
    for thread in result_dict:
        median_dict[thread] = transform_mv_result(result_dict[thread])
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

def write_searches_top():
    result_dir = "results/final/search/2_9/1/"
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
        if len(times) > 2:
            t0 = times[num_entries-1]["time"]
            t1 = times[num_entries-2]["time"]
            t2 = times[num_entries-3]["time"]
            return t0 > t1 and t1 > t2
    return False
    
def search_best_inner(expt, theta, out_dir):    
    thread_range = [4,8,12,16,20,24,28,32,36,40]
    prev_best = 0
    for t in thread_range:
        filename = str(t) + ".txt"
        outfile = os.path.join(out_dir, filename)
        if prev_best > 2:
            ccthreads = prev_best-2
        else:
            ccthreads = 1
        while ccthreads < t and not check_increasing(outfile):
            worker_threads = t-ccthreads
            mv_expt_single(out_dir, filename, ccthreads, 1000000, 1000000,
                           worker_threads, expt, 1, theta, 1000)            
            ccthreads += 1
        prev_best = check_best(outfile)

    top_level = "results/final/txn_size_10"

            
def search_best():
    low_contention = "results/final/txn_size_10/0_0/bohm"
    high_contention = "results/final/txn_size_10/0_9/bohm"
    for i in range(0, 5):
        temp = os.path.join(low_contention, str(i))
        search_best_inner(0, 0.0, temp)
        temp = os.path.join(high_contention, str(i))
        search_best_inner(0, 0.9, temp)


if __name__ == "__main__":
    main()
