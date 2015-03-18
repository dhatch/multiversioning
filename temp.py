#!/usr/bin/python
import os
import sys


cmd = "./build/db --cc_type 2 --num_lock_threads {0}  --num_txns 10000000 --epoch_size 10000 --num_records 1000000 --num_contended 2 --txn_size 10 --experiment 0 --record_size 1000 --distribution 1 --theta 0.0 --occ_epoch 8000000 --read_pct 0 --read_txn_size 5"

def main():
    for i in range(0, 5):
        

if __name__ == "__main__":
    main()
