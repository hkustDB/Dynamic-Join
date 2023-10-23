import os
import sys
import getopt
from multiprocessing import Pool

def main(argv):
    dataset = "roadNet-CA"
    query_name = "two_path"
    epsilon = 4.0
    Tau = 32768
    try:
        opts, args = getopt.getopt(argv, "d:q:e:T:", ["dataset=", "query_name", "epsilon=", "Tau="])
    except getopt.GetoptError:
        print("BinaryMechanism.py -d <dataset> -q <query_name> -e <epsilon> -T <Tau>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
        elif opt in ("-e", "--epsilon"):
            epsilon = float(arg)
        elif opt in ("-T", "--Tau"):
            tau = int(arg)
    if dataset in ["roadNet-CA", "dimacs9-USA", "wikipedia_link_nl", "wikipedia_link_ca", "dblp_coauthor", "flickr", "sx-stackoverflow"]:
        if query_name in ["two_path", "three_path", "triangle"]:
            cmd = "python ../Code/ConGraphFT.py -d %s -q %s -e %s -T %s"%(dataset, query_name, epsilon, Tau)
        elif query_name in ["three_star", "four_star"]:
            cmd = "python ../Code/ConGraphFT\(Star\).py -d %s -q %s -e %s -T %s"%(dataset, query_name, epsilon, Tau)
    elif dataset in ["TPCH"]:
        if query_name in ["q7"]:
            cmd = "python ../Code/TPCHq7FT.py -d %s -q %s -e %s -T %s"%(dataset, query_name, epsilon, Tau)
        elif query_name in ["q9"]:
            cmd = "python ../Code/TPCHq9FT.py -d %s -q %s -e %s -T %s"%(dataset, query_name, epsilon, Tau)
    try:
        print("cmd: %s started"%cmd)
        os.system(cmd)
    except:
        print("cmd: %s fail to execute"%cmd)

if __name__ == '__main__':
    main(sys.argv[1:])