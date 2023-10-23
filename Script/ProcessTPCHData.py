import getopt
import os
import shutil
import sys
from tqdm import tqdm
import shutil

def ProcessTPCHData():
    if not os.path.exists('../Temp/TPCH'):
        os.mkdir('../Temp/TPCH')
        os.mkdir('../Temp/TPCH/intermediate/')
        os.mkdir('../Temp/TPCH/answer/')
    for file in input_file_path_list:
        shutil.copy(file, '../Temp/TPCH')
    

def RemoveFiles():
    shutil.rmtree('../Temp/TPCH')

def main(argv):
    global input_file_path_list
    input_file_path_list = []
    model = 0

    try:
        opts, args = getopt.getopt(argv, "m:h:", ["model=", "help="])
    except getopt.GetoptError:
        print("ProcessTPCHData.py -m <model:0(import)/1(clean)>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print("ProcessTPCHData.py -m <model:0(import)/1(clean)>")
            sys.exit()
        elif opt in ("-m","--model"):
            model = int(arg)

    if model != 0:
        RemoveFiles()
        return
    else:
        input_file_path_list.append("../Data/TPCH/customer.tbl")
        input_file_path_list.append("../Data/TPCH/lineitem.tbl")
        input_file_path_list.append("../Data/TPCH/nation.tbl")
        input_file_path_list.append("../Data/TPCH/orders.tbl")
        input_file_path_list.append("../Data/TPCH/part.tbl")
        input_file_path_list.append("../Data/TPCH/partsupp.tbl")
        input_file_path_list.append("../Data/TPCH/region.tbl")
        input_file_path_list.append("../Data/TPCH/supplier.tbl")

        ProcessTPCHData()

if __name__ == '__main__':
    main(sys.argv[1:])