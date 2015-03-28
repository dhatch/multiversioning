#!/usr/bin/python

import os
import sys
import math

def confidence_interval(vals):
#    vals.sort()
#    cutoff = int(float(len(vals)) * 0.7)
#    temp = vals[0:cutoff]
#    vals = temp
    sample_mean = sum(vals) / float(len(vals))
    dev = 0
    for v in vals:
        dev += (sample_mean - v) * (sample_mean - v)
    s = math.sqrt(dev / float(len(vals)))
    conf_min = sample_mean - 1.96 * s
    conf_max = sample_mean + 1.96 * s
    return [sample_mean/float(1000), conf_min/float(1000), conf_max/float(1000)]

def compute_avg_records(input_file):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("threads:"):
                threads_str = s[len("threads:"):]
                threads = int(threads_str)
            elif s.startswith("theta:"):
                theta_str = s[len("theta:"):]
                theta = float(theta_str)
            elif s.startswith("records:"):
                records_str = s[len("records:"):]
                records = int(records_str)
                contention = 1.0 / float(records)

        throughput = (1.0*txns)/time
        if not contention in throughput_dict:
            throughput_dict[contention] = []            
        throughput_dict[contention].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    return throughput_dict



def compute_avg_locking_theta(input_file):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("threads:"):
                threads_str = s[len("threads:"):]
                threads = int(threads_str)
            elif s.startswith("theta:"):
                theta_str = s[len("theta:"):]
                theta = float(theta_str)
        throughput = (1.0*txns)/time
        if not theta in throughput_dict:
            throughput_dict[theta] = []            
        throughput_dict[theta].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    return throughput_dict


def compute_avg_mv_theta(input_file):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        threads = 0
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("ccthreads:"):
                threads_str = s[len("ccthreads:"):]
                threads += int(threads_str)
            elif s.startswith("workerthreads:"):
                threads_str = s[len("workerthreads:"):]
                threads += int(threads_str)
            elif s.startswith("theta:"):
                theta_str = s[len("theta:"):]
                theta = float(theta_str)
        throughput = (1.0*txns)/time
        if not theta in throughput_dict:
            throughput_dict[theta] = []            
        throughput_dict[theta].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    return throughput_dict
    

def compute_avg_mv(input_file, only_worker):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        threads = 0
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("ccthreads:") and (not only_worker):
                threads_str = s[len("ccthreads:"):]
                threads += int(threads_str)
            elif s.startswith("workerthreads:"):
                threads_str = s[len("workerthreads:"):]
                threads += int(threads_str)
        throughput = (1.0*txns)/time
        if not threads in throughput_dict:
            throughput_dict[threads] = []            
        throughput_dict[threads].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    return throughput_dict

def compute_avg_locking(input_file):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("threads:"):
                threads_str = s[len("threads:"):]
                threads = int(threads_str)
        throughput = (1.0*txns)/time
        if not threads in throughput_dict:
            throughput_dict[threads] = []            
        throughput_dict[threads].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    return throughput_dict

def write_output(output_dict, output_filename):
    outpt = open(output_filename, 'w')
    keys = output_dict.keys()
    print keys
    keys.sort()    
    for k in keys:
        output_lst = confidence_interval(output_dict[k])
        output_line = str(k) + " " + str(output_lst[0]) + " " + str(output_lst[1]) + " " + str(output_lst[2]) + "\n"
        outpt.write(output_line)
    outpt.close()

def theta_fn(input_type, input_file, output_file):
    if input_type == "locking":
        my_dict = compute_avg_locking_theta(input_file)
    elif input_type == "mv":
        my_dict = compute_avg_mv_theta(input_file)
    write_output(my_dict, output_file)

def clean_fn(input_type, input_file, output_file, only_worker=False):
    if input_type == "locking":
        my_dict = compute_avg_locking(input_file)
    elif input_type == "mv":        
        my_dict = compute_avg_mv(input_file, only_worker)
    elif input_type == "occ":
        my_dict = compute_avg_locking(input_file)
    write_output(my_dict, output_file)

def list_times(input_file):
    ret = []
    inpt = open(input_file)
    for line in inpt:
        splits = line.split()
        cur = {}
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
                cur["time"] = time
            elif s.startswith("ccthreads:"):
                threads_str = s[len("ccthreads:"):]
                threads = int(threads_str)
                cur["threads"] = threads
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
                cur["txns"] = txns
            if "threads" in cur and "time" in cur and "txns" in cur:
                ret.append(cur)
                break
    inpt.close()
    return ret

def cc_throughput(input_file):
    inpt = open(input_file)
    throughput_dict = {}
    for line in inpt:
        threads = 0
        splits = line.split()
        for s in splits:
            if s.startswith("time:"):
                time_str = s[len("time:"):]
                time = float(time_str)
            elif s.startswith("txns:"):
                txns_str = s[len("txns:"):]
                txns = int(txns_str)
            elif s.startswith("ccthreads:"):
                threads_str = s[len("ccthreads:"):]
                threads = int(threads_str)
        throughput = (1.0*txns)/time
        if not threads in throughput_dict:
            throughput_dict[threads] = []            
        throughput_dict[threads].append(throughput)

    for key in throughput_dict:
        thpt_list = throughput_dict[key]
        thpt_list.sort()
    inpt.close()
    print throughput_dict
    return throughput_dict

def cc_fn(input_file, output_file):
    my_dict = cc_throughput(input_file)
    write_output(my_dict, output_file)

def records_fn(input_file, output_file):
    my_dict = compute_avg_records(input_file)
    write_output(my_dict, output_file)

def main():
    records_fn(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
    main()

    
    
