import getopt
import os
import shutil
import sys
from tqdm import tqdm

def ProcessGraphData():
    idx = 0
    count = 0
    nodes = {}

    if not os.path.exists('../Temp/' + dataset):
        os.mkdir('../Temp/' + dataset)
        os.mkdir('../Temp/' + dataset + '/intermediate/')
        os.mkdir('../Temp/' + dataset + '/answer/')
        os.mkdir('../Temp/' + dataset + '/original/')
    reindex_graph_path = '../Temp/' + dataset + '/original/' + dataset + '.csv'

    input_file = open(input_file_path, 'r')
    reindex_graph = open(reindex_graph_path, 'w')

    # Re-index the nodes to make it start from 0
    for line in tqdm(input_file.readlines()):
        temp_line = line.strip().split('\t')
        if len(temp_line) == 1:
            continue
        if temp_line[0] not in nodes:
            nodes[temp_line[0]] = idx
            temp_line[0] = str(nodes[temp_line[0]])
            idx += 1
            count += 1
            if temp_line[1] not in nodes:
                nodes[temp_line[1]] = idx
                temp_line[1] = str(nodes[temp_line[1]])
                idx += 1
                count += 1
            else:
                temp_line[1] = str(nodes[temp_line[1]])
        elif temp_line[1] not in nodes:
            temp_line[0] = str(nodes[temp_line[0]])
            nodes[temp_line[1]] = idx
            temp_line[1] = str(nodes[temp_line[1]])
            idx += 1
            count += 1
        elif (temp_line[0] in nodes) and (temp_line[1] in nodes):
            temp_line[0] = str(nodes[temp_line[0]])
            temp_line[1] = str(nodes[temp_line[1]])

        # Concate n|v|time forms a line
        splited_string = ""
        for i in range(2):
            splited_string += temp_line[i] + "|"
        splited_string = splited_string[:-1]

        reindex_graph.write(splited_string)
        reindex_graph.write('\n')
    reindex_graph.close()
    input_file.close()

def RemoveFiles():
    shutil.rmtree('../Temp/' + dataset)

def main(argv):
    global dataset
    dataset = ""
    global input_file_path
    input_file_path = ""
    model = 0

    try:
        opts, args = getopt.getopt(argv, "d:m:h:", ["dataset=", "model=", "help="])
    except getopt.GetoptError:
        print("ProcessGraphData.py -d <dataset> -m <model:0(import)/1(clean)>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("ProcessGraphData.py -d <dataset> -m <model:0(import)/1(clean)>")
            sys.exit()
        elif opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-m","--model"):
            model = int(arg)

    if model != 0:
        RemoveFiles()
        return
    else:
        input_file_path = "../Data/Graph/" + dataset
        ProcessGraphData()

if __name__ == '__main__':
    main(sys.argv[1:])