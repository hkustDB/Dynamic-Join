import getopt
import os
import sys
from tqdm import tqdm
import random

def RandomOrder(input_file_path, output_file_path):
    input_file_path = input_file_path
    input_file = open(input_file_path, 'r')
    list_of_list = []
    timestamp = ""
    tuples_with_same_timestamp = []
    for line in tqdm(input_file.readlines()):
        elements = line.strip().split('\t')
        if timestamp == str(elements[2]):
            if str(elements[2]) == "-1":
                tuples_with_same_timestamp.append((elements[1], elements[0]))
            else:
                tuples_with_same_timestamp.append((elements[0], elements[1]))
        else:
            timestamp = str(elements[2])
            list_of_list.append(tuples_with_same_timestamp)
            tuples_with_same_timestamp = []
            if str(elements[2]) == "-1":
                tuples_with_same_timestamp.append((elements[1], elements[0]))
            else:
                tuples_with_same_timestamp.append((elements[0], elements[1]))
    list_of_list.append(tuples_with_same_timestamp)
    input_file.close()
    for tuples_list in list_of_list:
        random.shuffle(tuples_list)
    output_file = open(output_file_path, 'w')
    for tuples_list in list_of_list:
        for tuple in tuples_list:
            writeline = str(tuple[0]) + '\t' + str(tuple[1])
            output_file.write(writeline + '\n')
    output_file.close()

def main(argv):
    input_file_path = "../Data/dataset_name/original/dataset_name"
    output_file_path = "../Data/dataset_name/dataset_name"
    RandomOrder(input_file_path, output_file_path)

if __name__ == '__main__':
    main(sys.argv[1:])