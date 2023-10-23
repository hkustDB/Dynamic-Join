import getopt
import os
import sys
from tqdm import tqdm


def DeleteTopNodes(ratio, input_file_path, output_file_path):
    ratio = ratio
    input_file_path = input_file_path
    input_file = open(input_file_path, 'r')
    node_count = {}
    for line in tqdm(input_file.readlines()):
        elements = line.strip().split('|')
        if elements[0] in node_count.keys():
            node_count[elements[0]] += 1
        else:
            node_count[elements[0]] = 1
        if elements[1] in node_count.keys():
            node_count[elements[1]] += 1
        else:
            node_count[elements[1]] = 1
    delete_nodes = sorted(node_count.items(), key=lambda x: x[1], reverse=True)
    delete_nodes = delete_nodes[0:int(len(delete_nodes) * ratio)]
    cut_degree = delete_nodes[-1][1]
    print(cut_degree)

    input_file.seek(0)
    output_file_path = output_file_path
    output_file = open(output_file_path, "w")
    for line in tqdm(input_file.readlines()):
        elements = line.strip().split('|')
        from_id = elements[0]
        to_id = elements[1]
        if node_count[from_id] >= cut_degree or node_count[to_id] >= cut_degree:
            continue
        else:
            writeline = str(from_id) + '|' + str(to_id)
            output_file.write(writeline + '\n')
    input_file.close()
    output_file.close()

def main(argv):
    global dataset
    dataset = ""
    global ratio
    ratio = 0.05
    global input_file_path
    input_file_path = ""
    global output_file_path
    output_file_path = ""

    try:
        opts, args = getopt.getopt(argv, "d:r:h:", ["dataset=", "ratio=", "help="])
    except getopt.GetoptError:
        print("DeleteTopNodes.py -d <dataset> -r <ratio (default 0.05)>")
    for opt, arg in opts:
        if opt == '-h':
            print("DeleteTopNodes.py -d <dataset> -r <ratio (default 0.05)>")
            sys.exit()
        elif opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-r", "--ratio"):
            ratio = float(arg)
    
    input_file_path = "../Temp/" + dataset + '/' + "original" + '/' + dataset + ".csv"
    output_file_path = "../Temp/" + dataset + '/' + dataset + ".csv"
    DeleteTopNodes(ratio, input_file_path, output_file_path)

if __name__ == '__main__':
    main(sys.argv[1:])