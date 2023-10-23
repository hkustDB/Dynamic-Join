import os
import sys
import subprocess
from multiprocessing import Pool

def execCmd(cmd):
    try:
        print("cmd: %s started%"%cmd)
    except:
        print("cmd: %s fail to execute"%cmd)

def main(argv):
    datasets = ["roadNet-CA", "dimacs9-USA", "wikipedia_link_nl", "wikipedia_link_ca", "dblp_coauthor", "flickr", "sx-stackoverflow"]
    queries_name = ["two_path", "three_path", "triangle", "three_star", "four_star"]

    for query_name in queries_name:
        for dataset in datasets:
            cmd = "python ../Script/CollectStatDP.py -d %s -q %s"%(dataset, query_name)
            try:
                args = ['../Script/CollectStatDP.py', '-d', dataset, '-q', query_name]
                output = subprocess.check_output(['python'] + args)
                os.system(cmd)
            except:
                print("cmd: %s fail to execute"%cmd)

if __name__ == '__main__':
    main(sys.argv[1:])