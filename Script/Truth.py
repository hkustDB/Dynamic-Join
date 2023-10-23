import os
import sys
import getopt
from multiprocessing import Pool

def main(argv):
    dataset = "roadNet-CA"
    query_name = "two_path"
    try:
        opts, args = getopt.getopt(argv, "d:q:", ["dataset=", "query_name"])
    except getopt.GetoptError:
        print("Truth.py -d <dataset> -q <query_name>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
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
    try:
        print("cmd: %s started"%cmd)
        os.system(cmd)
    except:
        print("cmd: %s fail to execute"%cmd)

if __name__ == '__main__':
    main(sys.argv[1:])