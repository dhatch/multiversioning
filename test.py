#!/usr/bin/python

import os
import sys

sys.path.append('results/rec_1000/uncontended/')

import clean

def main():
    clean.clean_fn("mv", "results.txt", "blah.txt")

if __name__ == "__main__":
    main()
