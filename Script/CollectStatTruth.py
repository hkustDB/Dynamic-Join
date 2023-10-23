import getopt
import math
import sys
import numpy as np
import time
import os
import re

def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = ""

    try:
        opts, args = getopt.getopt(argv, "d:q:e:",["dataset=", "query_name="])
    except getopt.GetoptError:
        print("CollectTruthStat.py")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
    answer_file_prefix = "../Experiment/" + dataset + '/'
    return dataset, query_name, answer_file_prefix

def CollectTruth(answer_file_prefix):
    truth_path = answer_file_prefix + '/Truth/' + query_name + '/'
    truthdir = os.listdir(truth_path)
    if truthdir[0] == '.DS_Store':
        truth_file_path = truth_path + truthdir[1]
    else:
        truth_file_path = truth_path + truthdir[0]
    truth_file = open(truth_file_path, 'r')
    timestamp_list = []
    truth_list = []
    for line in truth_file.readlines()[1:-1]:
        elements = line.strip().split('|')
        timestamp_list.append(int(elements[0]))
        truth_list.append(float(elements[1]))
    truth_file.close()

    stat_file = open('../Stat/' + dataset + '_' + query_name + '_Truth_stat.txt', 'w')
    writeline = 'dataset=%s, query_name=%s (Truth)'%(dataset, query_name)
    stat_file.write(writeline + '\n')
    stat_file.write(str(truth_list))
    stat_file.close()

def main(argv):
    dataset, query_name, answer_file_prefix = loaddata(sys.argv[1:])
    CollectTruth(answer_file_prefix)

if __name__ == '__main__':
    main(sys.argv[1:])