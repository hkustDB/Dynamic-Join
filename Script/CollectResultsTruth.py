# import datetime
import os
import sys
import threading
import getopt
import subprocess
from multiprocessing import Pool

def execCmd(cmd):
    try:
        print("cmd: %s started"%cmd)
        subprocess.call(cmd, shell=True)
    except:
        print("cmd: %s fail to execute"%cmd)

def main(argv):
    dataset = "roadNet-CA"
    query_name = "two_path"
    try:
        opts, args = getopt.getopt(argv, "d:q:b:t:", ["dataset=", "query_name="])
    except getopt.GetoptError:
        print("CollectResultsTruth.py -d <dataset> -q <query_name>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)


    cmd_pool = Pool(processes=10)
    if dataset in ["roadNet-CA", "dimacs9-USA", "wikipedia_link_nl", "wikipedia_link_ca", "dblp_coauthor", "flickr", "sx-stackoverflow"]:
        if query_name in ["two_path", "three_path", "triangle"]:
            cmd = "python ../Code/ConGraphTruth.py -d %s -q %s"%(dataset, query_name)
        elif query_name in ["three_star", "four_star"]:
                cmd = "python ../Code/ConGraphTruth\(Star\).py -d %s -q %s"%(dataset, query_name)
    elif dataset in ["TPCH"]:
        if query_name in ["q7"]:
            cmd = "python ../Code/TPCHq7Truth.py -d %s -q %s"%(dataset, query_name)
        elif query_name in ["q9"]:
            cmd = "python ../Code/TPCHq9Truth.py -d %s -q %s"%(dataset, query_name)
    cmd_pool.apply_async(execCmd, args=(cmd,))
    cmd_pool.close()
    cmd_pool.join()

if __name__ == '__main__':
    main(sys.argv[1:])