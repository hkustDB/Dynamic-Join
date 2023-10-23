import os
import sys
import threading
import getopt
import subprocess
from multiprocessing import Pool

def main(argv):
    dataset = "roadNet-CA"
    query_name = "two_path"
    epsilon = 4.0
    beta = 0.1
    theta = 1
    try:
        opts, args = getopt.getopt(argv, "d:q:e:b:t:", ["dataset=", "query_name", "epsilon=", "beta=", "theta="])
    except getopt.GetoptError:
        print("DPMechanism.py -d <dataset> -q <query_name> -e <epsilon(default 4)> -b <beta(default 0.1)> -t <theta(default 1)>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
        elif opt in ("-e", "--epsilon"):
            epsilon = float(arg)
        elif opt in ("-b", "--beta"):
            beta = float(arg)
        elif opt in ("-t", "--theta"):
            theta = float(arg)
    if dataset in ["roadNet-CA", "dimacs9-USA", "wikipedia_link_nl", "wikipedia_link_ca", "dblp_coauthor", "flickr", "sx-stackoverflow"]:
        if query_name in ["two_path", "three_path", "triangle"]:
            cmd = "python ../Code/ConGraphDP.py -d %s -q %s -e %s -b %s -t %s"%(dataset, query_name, epsilon, beta, theta)
        elif query_name in ["three_star", "four_star"]:
            cmd = "python ../Code/ConGraphDP\(Star\).py -d %s -q %s -e %s -b %s -t %s"%(dataset, query_name, epsilon, beta, theta)
    elif dataset in ["TPCH"]:
        if query_name in ["q7"]:
            cmd = "python ../Code/TPCHq7DP.py -d %s -q %s -e %s -b %s -t %s"%(dataset, query_name, epsilon, beta, theta)
        elif query_name in ["q9"]:
            cmd = "python ../Code/TPCHq9DP.py -d %s -q %s -e %s -b %s -t %s"%(dataset, query_name, epsilon, beta, theta)
    try:
        print("cmd: %s started"%cmd)
        os.system(cmd)
    except:
        print("cmd: %s fail to execute"%cmd)

if __name__ == '__main__':
    main(sys.argv[1:])