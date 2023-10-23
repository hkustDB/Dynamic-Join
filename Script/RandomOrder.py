import getopt
import os
import sys
from tqdm import tqdm
import random

def RandomOrder(input_file_path, output_file_path):
    input_file_path = input_file_path
    input_file = open(input_file_path, 'r')
    whole_list = []
    count = 0
    for line in tqdm(input_file.readlines()):
        count += 1
        if count == 1:
            continue
        elements = line.strip().split('\t')
        whole_list.append((elements[0], elements[1]))
    input_file.close()
    random.shuffle(whole_list)
    output_file_path = output_file_path
    output_file = open(output_file_path, 'w')
    for elements in whole_list:
        writeline = str(elements[0]) + '\t' + str(elements[1])
        output_file.write(writeline + '\n')
    output_file.close()

def main(argv):
    input_file_path = "../Data/dataset_name/original/dataset_name"
    output_file_path = "../Data/dataset_name/dataset_name"
    RandomOrder(input_file_path, output_file_path)

if __name__ == '__main__':
    main(sys.argv[1:])