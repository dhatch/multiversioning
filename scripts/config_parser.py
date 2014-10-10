#!/usr/bin/python

import os
import sys

BIN="../build/db"

def get_single_param(param_string):
    ret = ""
    parts = param_string.split()
    assert(len(parts) == 0 or len(parts) == 1)
    param_name = line_parts[0]
    
    if len(param_name) == 1:
        ret += " -"
    else:
        ret += " --"
    ret += param_name
    
    if len(parts) == 2:
        ret += " " + parts[1]
    return ret

def parse_lines(filename):
    input_file = open(filename)
    param_string = ""

    for line in input_file:
        param_string += get_single_param(line)
        
    input_file.close()
    return param_string

def main():
    config_file = sys.argv[1]
    params = parse_lines(config_file)
    os.system(BIN + params)
    

if __name__ == "__main__":
    main()
