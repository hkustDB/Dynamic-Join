import getopt
import math
import sys
import numpy as np
import time
import os

def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = ""

    try:
        opts, args = getopt.getopt(argv, "d:q:e:",["dataset=","query_name="])    
    except getopt.GetoptError:
        print("CollectDPStat.py")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
    answer_file_prefix = "../Experiment/" + dataset + '/'
    return dataset, query_name, answer_file_prefix

def CollectDPError(answer_file_prefix):
    res_path = answer_file_prefix + 'DP/' + query_name + '/'
    listdir = os.listdir(res_path)
    if len(listdir) == 0:
        return
    timestamp_list = []
    truth_list = []
    total_noised_query_result = []
    total_cost_time = []
    total_query_result = []
    total_noise_result = []

    truth_path = answer_file_prefix + '/Truth/' + query_name + '/'
    truthdir = os.listdir(truth_path)
    if truthdir[0] == '.DS_Store':
        truth_file_path = truth_path + truthdir[1]
    else:
        truth_file_path = truth_path + truthdir[0]
    truth_file = open(truth_file_path, 'r')
    timestamp_list = []
    truth_list = []
    for line in truth_file.readlines()[1:-1]:
        elements = line.strip().split('|')
        timestamp_list.append(int(elements[0]))
        truth_list.append(float(elements[1]))
    truth_file.close()

    for file_name in listdir:
        noised_query_result_list = []
        cost_time_list = []
        query_result_list = []
        noise_result_list = []
        file_path = res_path + file_name
        file = open(file_path, 'r')
        if file_name == '.DS_Store':
            continue
        for line in file.readlines()[1:-1]:
            elements = line.strip().split('|')
            noised_query_result_list.append(float(elements[1]))
            cost_time_list.append(float(elements[2][:-1]))
            query_result_list.append(float(elements[3]))
            noise_result_list.append(float(elements[4]))
        total_noised_query_result.append(noised_query_result_list)
        total_cost_time.append(cost_time_list)
        total_query_result.append(query_result_list)
        total_noise_result.append(noise_result_list)

    total_noised_query_result = np.array(total_noised_query_result).T
    total_cost_time = np.array(total_cost_time).T
    total_query_result = np.array(total_query_result).T
    total_noise_result = np.array(total_noise_result).T

    # Remove 20% largest and 20% smallest error value
    new_total_noised_query_result = []
    for index in range(len(timestamp_list)):
        truth = truth_list[index]
        noised_query_result_list = total_noised_query_result[index]
        num_to_remove = int(len(noised_query_result_list) * 0.2)
        sorted_noised_query_result_list = sorted(noised_query_result_list, key=lambda x: abs(x - truth), reverse=True)
        new_noised_query_result_list = sorted_noised_query_result_list[num_to_remove:]
        new_noised_query_result_list = new_noised_query_result_list[:-num_to_remove]
        new_total_noised_query_result.append(new_noised_query_result_list)

    stat_file = open('../Stat/' + dataset + '_' + query_name + '_Ours_stat.txt', 'w')
    writeline = 'dataset=%s, query_name=%s (Ours)'%(dataset, query_name)
    stat_file.write(writeline + '\n')
    ave_error_list = []
    ave_relative_error_list = []
    ave_cost_time_list = []
    for index in range(len(timestamp_list)):
        truth = truth_list[index]
        length = len(new_total_noised_query_result[index])
        ave_error = np.linalg.norm(np.array([truth] * length) - np.array(new_total_noised_query_result[index]), ord=1) / length
        ave_error_list.append(ave_error)
        relative_error_list = []
        for sample_index in range(len(new_total_noised_query_result[index])):
            relative_error_list.append(abs(new_total_noised_query_result[index][sample_index]-truth)/truth)
        ave_relative_error = np.mean(relative_error_list)
        ave_relative_error_list.append(ave_relative_error)
        ave_cost_time = np.linalg.norm(np.array(total_cost_time[index]), ord=1) / len(total_cost_time[index])
        ave_cost_time_list.append(ave_cost_time)
        ave_query_result = np.linalg.norm(np.array(total_query_result[index]), ord=1) / len(total_query_result[index])
        ave_noise_result = np.linalg.norm(np.array(total_noise_result[index]), ord=1) / len(total_noise_result[index])
        writeline = "Timestamp: %s, Ave relative error:%s, Ave cost timme:%s, Ave L1 norm error:%s, Ave res:%s, Ave L1 norm noise:%s\n"%(timestamp_list[index], ave_relative_error_list[index], ave_cost_time, ave_error, ave_query_result, ave_noise_result)
        stat_file.write(writeline)
    stat_file.write("Relative Error list:%s"%ave_relative_error_list + '\n')
    ave_relative_error_list = [x for x in ave_relative_error_list if not math.isnan(x)]
    clipped_ave_relative_error_list = ave_relative_error_list
    stat_file.write("90%% Relative Error:%s"%sorted(clipped_ave_relative_error_list)[int(0.9*len(clipped_ave_relative_error_list))] + '\n')
    stat_file.write("median Relative Error:%s"%sorted(clipped_ave_relative_error_list)[int(0.5*len(clipped_ave_relative_error_list))] + '\n')
    stat_file.write("Average Running Time:%ss"%np.mean(ave_cost_time_list) + '\n')
    stat_file.write(str(ave_error_list))
    info = ['dataset=%s, query_name=%s, meadian error=%e, running time:%s'%(dataset, query_name, float(sorted(clipped_ave_relative_error_list)[int(0.5*len(clipped_ave_relative_error_list))]), np.mean(ave_cost_time_list))]
    print(info)
    stat_file.close()

def main(argv):
    dataset, query_name, answer_file_prefix = loaddata(sys.argv[1:])
    CollectDPError(answer_file_prefix)
        
if __name__ == '__main__':
    main(sys.argv[1:])