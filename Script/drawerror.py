# -*- coding: utf-8 -*-
import matplotlib.pyplot as plt
import sys
import os
import numpy as np
import matplotlib as mpl
mpl.use('TkAgg')
import getopt

def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = ""
    global epsilon
    epsilon = 4
    global stat_file_prefix
    
    try:
        opts, args = getopt.getopt(argv, "d:q:e:",['dataset=', "query_name=", "epsilon="])
    except getopt.GetoptError:
        print('draw graph wrong')
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
    stat_file_prefix = "../Experiment/Stat/" + dataset + '/' + query_name + "/"
    return stat_file_prefix

def getOurs():
    file_name_list = sorted(os.listdir(stat_file_prefix))
    Ours_list_path = stat_file_prefix + file_name_list[2]
    Ours_list_file = open(Ours_list_path, 'r')
    lines = Ours_list_file.readlines()
    last_line = lines[-1]
    Our_list = eval(last_line)
    return Our_list

def getBM():
    file_name_list = sorted(os.listdir(stat_file_prefix))
    BM_list_path = stat_file_prefix + file_name_list[0]
    BM_list_file = open(BM_list_path, 'r')
    lines = BM_list_file.readlines()
    last_line = lines[-1]
    BM_list = eval(last_line)
    return BM_list

def getCM():
    file_name_list = sorted(os.listdir(stat_file_prefix))
    CM_list_path = stat_file_prefix + file_name_list[1]
    CM_list_file = open(CM_list_path, 'r')
    lines = CM_list_file.readlines()
    last_line = lines[-1]
    CM_list = eval(last_line)
    return CM_list

def getRS():
    file_name_list = sorted(os.listdir(stat_file_prefix))
    RS_list_path = stat_file_prefix + file_name_list[3]
    RS_list_file = open(RS_list_path, 'r')
    lines = RS_list_file.readlines()
    last_line = lines[-1]
    RS_list = eval(last_line)
    return RS_list

def getTruth():
    file_name_list = sorted(os.listdir(stat_file_prefix))
    Truth_list_path = stat_file_prefix + file_name_list[4]
    Truth_list_file = open(Truth_list_path, 'r')
    lines = Truth_list_file.readlines()
    last_line = lines[-1]
    Truth_list = eval(last_line)
    return Truth_list

def draw(Ours_list, BM_list, CM_list, RS_list, Truth_list):
    
    Ours = Ours_list
    BM = BM_list
    CM = CM_list
    RS = RS_list
    Truth = Truth_list
    
    if dataset in ('dimacs9-USA'):
        x = [i for i in range(1, 11)]
        xlabels = [ str(i*4) + 'M' for i in x] # case by case
    elif dataset in ('wikipedia_link_nl', 'wikipedia_link_ca', 'dblp_coauthor', 'sx-stackoverflow'):
        x = [i for i in range(1, 11)]
        xlabels = [ str(i*1) + 'M' for i in x] # case by case
    elif dataset in ('flickr'):
        x = [i for i in range(1, 9)]
        xlabels = [ str(i*0.4)[:3] + 'M' for i in x] # case by case
    elif dataset in ('roadNet-CA'):
        x = [i for i in range(1, 6)]
        xlabels = [ str(i*0.25)[0:3] + 'M' for i in x] # case by case


    Ours_ = []
    BM_ = []
    CM_ = []
    RS_ = []
    Truth_ = []
    plt.figure(figsize=(10, 5))
    if dataset in ('dimacs9-USA', 'wikipedia_link_nl', 'wikipedia_link_ca', 'dblp_coauthor', 'sx-stackoverflow'):
        for i in range(10):
            slice_size = len(Ours) / 10
            start_index = int(i * slice_size)
            end_index = int((i + 1) * slice_size)
            Ours_.append(Ours[start_index:end_index][0])
            BM_.append(BM[start_index:end_index][0])
            CM_.append(CM[start_index:end_index][0])
            RS_.append(RS[start_index:end_index][0])
            Truth_.append(Truth[start_index:end_index][0])
    elif dataset in ('flickr'):
        for i in range(8):
            slice_size = len(Ours) / 8
            start_index = int(i * slice_size)
            end_index = int((i + 1) * slice_size)
            Ours_.append(Ours[start_index:end_index][0])
            BM_.append(BM[start_index:end_index][0])
            CM_.append(CM[start_index:end_index][0])
            RS_.append(RS[start_index:end_index][0])
            Truth_.append(Truth[start_index:end_index][0])
    elif dataset in ('roadNet-CA'):
        for i in range(5):
            slice_size = len(Ours) / 5
            start_index = int(i * slice_size)
            end_index = int((i + 1) * slice_size)
            Ours_.append(Ours[start_index:end_index][0])
            BM_.append(BM[start_index:end_index][0])
            CM_.append(CM[start_index:end_index][0])
            RS_.append(RS[start_index:end_index][0])
            Truth_.append(Truth[start_index:end_index][0])

    plt.tick_params(axis='both', which='major', labelsize=8)

    plt.plot(x, BM_,linewidth = 1,linestyle = '-.',label='BM',
            marker = 's',markersize = 2.5,color=plt.cm.tab20c(4),
            markeredgecolor=plt.cm.tab20c(4),markeredgewidth = 1,markerfacecolor=plt.cm.tab20c(4))
    plt.plot(x, CM_,linewidth = 1,linestyle = ':',label='CM',
            marker = 'o',markersize = 2.5,color=plt.cm.tab20c(12),
            markeredgecolor=plt.cm.tab20c(12),markeredgewidth = 1,markerfacecolor='white')
    plt.plot(x, RS_,linewidth = 1, linestyle = '--',label='RS',
            marker = 'v',markersize = 2.5,color=plt.cm.tab20c(16),
            markeredgecolor=plt.cm.tab20c(16),markeredgewidth = 1,markerfacecolor=plt.cm.tab20c(16))
    plt.plot(x, Truth_, linewidth = 1, linestyle = ':', label='Query result',
            marker = 'h', markersize = 2.5, color = plt.cm.tab20c(8),
            markeredgecolor=plt.cm.tab20c(8), markeredgewidth = 1, markerfacecolor = plt.cm.tab20c(8))
    plt.plot(x, Ours_, linewidth = 1, linestyle = '-.',label='Ours',
            marker = 's',markersize = 2.5,color=plt.cm.tab20c(0),
            markeredgecolor=plt.cm.tab20c(0),markeredgewidth = 1,markerfacecolor=plt.cm.tab20c(0))

    plt.yscale('log')
    
    plt.ylabel("Error Level",fontsize=12)
    plt.xlabel("Timestamp",fontsize=12)

    plt.xticks(x, xlabels)
    

    if epsilon == '2' and query_name == "three_path":
        plt.legend(loc='upper left', bbox_to_anchor=(0,0.9), fontsize=8, ncol=1, frameon=False)
    
    title = "../Image/eps=4/" + query_name + '_' + dataset + '_' 'epsilon=%s.png'%str(epsilon)
    plt.savefig(title, bbox_inches='tight')
    title = "../Image/eps=4/" + query_name + '_' + dataset + '_' 'epsilon=%s.pdf'%str(epsilon)
    plt.savefig(title, bbox_inches='tight')

if __name__ == "__main__":
    stat_file_prefix = loaddata(sys.argv[1:])
    Ours_list = getOurs()
    BM_list = getBM()
    CM_list = getCM()
    RS_list = getRS()
    Truth_list = getTruth()
    draw(Ours_list, BM_list, CM_list, RS_list, Truth_list)
