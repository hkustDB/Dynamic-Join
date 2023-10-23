import os
import sys
import getopt
from multiprocessing import Pool

def main(argv):
    dataset = "roadNet-CA"
    query_name = "two_path"
    epsilon = 4.0
    Delta = 10 ** -10
    try:
        opts, args = getopt.getopt(argv, "d:q:e:D:", ["dataset=", "query_name", "epsilon=", "Delta="])
    except getopt.GetoptError:
        print("RS.py -d <dataset> -q <query_name> -e <epsilon>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
        elif opt in ("-e", "--epsilon"):
            epsilon = float(arg)
        elif opt in ("-D", "--Delta"):
            Delta = float(arg)
    if dataset in ["roadNet-CA", "dimacs9-USA", "wikipedia_link_nl", "wikipedia_link_ca", "dblp_coauthor", "flickr", "sx-stackoverflow"]:
        cmd = "python ../Code/ConGraphRS.py -d %s -q %s -e %s -D %s"%(dataset, query_name, epsilon, Delta)
    elif dataset in ["TPCH"]:
        if query_name in ["q7"]:
            cmd = "python ../Code/TPCHq7RS.py -d %s -q %s -e %s -D %s"%(dataset, query_name, epsilon, Delta)
        elif query_name in ["q9"]:
            cmd = "python ../Code/TPCHq9RS.py -d %s -q %s -e %s -D %s"%(dataset, query_name, epsilon, Delta)
    try:
        print("cmd: %s started"%cmd)
        os.system(cmd)
    except:
        print("cmd: %s fail to execute"%cmd)

if __name__ == '__main__':
    main(sys.argv[1:])