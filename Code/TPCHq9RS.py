import getopt
import math
import sys
import GlobalSensitivity as GlobalSensitivity
import random
import time
import os
import psycopg2
from datetime import datetime
from tqdm import tqdm
import subprocess
import csv
import ast

def LapNoise():
    a = random.uniform(0, 1)
    b = math.log(1/(1-a))
    c = random.uniform(0, 1)
    if c > 0.5:
        return b
    else:
        return -b

def CauchyCum(x):
    a = 1/4/math.sqrt(2)*(math.log(abs(2*x**2+2*math.sqrt(2)*x+2))+2*math.atan(math.sqrt(2)*x+1))
    a += 1/4/math.sqrt(2)*(-math.log(abs(2*x**2-2*math.sqrt(2)*x+2))+2*math.atan(math.sqrt(2)*x-1))
    return a

def CauNoise():
    a = random.uniform(0,math.pi/2/math.sqrt(2))
    left = 0
    right = 6000
    mid = 1.0*(left+right)/2
    while(abs(CauchyCum(mid)-a)>0.000001):
        if CauchyCum(mid)>a:
            right = mid
        else:
            left = mid
        mid = 1.0*(left+right)/2
    c = random.uniform(0,1)
    if c>0.5:
        return mid
    else:
        return -mid

class TPCHq9RS:
    def __init__(self, relation_list, epsilon, beta, T, query_name, query=""):
        self.relation_list = relation_list
        self.epsilon = epsilon
        self.beta = beta
        self.T = T
        self.query_name = query_name
        self.query = self.Query_Instance(query)

        self.RS_cost_time = []
        self.RS = []
        self.TE = []
        self.timestamp_list= []
        self.query_result = []
        self.noised_result = []

        self.con, self.cur = self.ConnectPsql()
        self.CreateTables()

    def Query_Instance(self, query):
        query_instance = query
        for relation in self.relation_list:
            query_instance.replace(relation, relation+'_q9')
        return query_instance

    def ConnectPsql(self):
        con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        return con, cur

    def CreateTables(self):
        create_temp_table = "create table supplier_q9 (sk integer not null)"
        self.cur.execute(create_temp_table)
        create_temp_table = "create table partsupp_q9 (sk integer not null, pk integer not null)"
        self.cur.execute(create_temp_table)
        create_temp_table = "create table lineitem_q9 (sk integer not null, pk integer not null, ok integer not null)"
        self.cur.execute(create_temp_table)
        create_temp_table = "create table orders_q9 (ok integer not null)"
        self.cur.execute(create_temp_table)
        self.con.commit()

    def DropTables(self):
        drop_table = "drop table supplier_q9"
        self.cur.execute(drop_table)
        drop_table = "drop table partsupp_q9"
        self.cur.execute(drop_table)
        drop_table = "drop table lineitem_q9"
        self.cur.execute(drop_table)
        drop_table = "drop table orders_q9"
        self.cur.execute(drop_table)
        self.con.commit()

    # Use ResidualSensitivity.py to compute the RS
    def GetResidualSensitivity(self, footstep, random_list):
        C_count = 0
        O_count = 0
        L_count = 0
        S_count = 0
        for t in tqdm(range(1, self.T+1)):
            if random_list[t-1] == 'S': C_count += 1
            elif random_list[t-1] == 'PS': O_count += 1
            elif random_list[t-1] == 'L': L_count += 1
            elif random_list[t-1] == 'O': S_count += 1
            if t % footstep == 0:
                with open('../Temp/TPCH/supplier.tbl', 'r') as f:
                    reader = csv.reader(f, delimiter='|')
                    data = [(row[0],) for i, row in enumerate(reader) if i < C_count]
                self.cur.executemany("insert into supplier_q9 values (%s)", data)
                self.con.commit()
                with open('../Temp/TPCH/partsupp.tbl', 'r') as f:
                    reader = csv.reader(f, delimiter='|')
                    data = [(row[1], row[0]) for i, row in enumerate(reader) if i < O_count]
                self.cur.executemany("insert into partsupp_q9 values (%s, %s)", data)
                self.con.commit()
                with open('../Temp/TPCH/lineitem.tbl', 'r') as f:
                    reader = csv.reader(f, delimiter='|')
                    data = [(row[2], row[1], row[0]) for i, row in enumerate(reader) if i < L_count]
                self.cur.executemany("insert into lineitem_q9 values (%s, %s)", data)
                self.con.commit()
                with open('../Temp/TPCH/orders.tbl', 'r') as f:
                    reader = csv.reader(f, delimiter='|')
                    data = [(row[0],) for i, row in enumerate(reader) if i < S_count]
                self.cur.executemany("insert into orders_q9 values (%s)", data)
                self.con.commit()
                start = time.time()
                args = ['../Code/ResidualSensitivity.py', '-d', dataset, '-q', query_name, '-b', '%s'%self.beta, '-u', '%s'%psql_user, '-p', '%s'%psql_password, '-h', '%s'%psql_host, '-o', '%s'%psql_port]
                output = subprocess.check_output(['python'] + args)
                result, TE = ast.literal_eval(output.decode().strip())
                self.RS.append(result)
                self.TE.append(TE)
                end = time.time()
                truncate_table = "truncate table supplier_q9"
                self.cur.execute(truncate_table)
                truncate_table = "truncate table partsupp_q9"
                self.cur.execute(truncate_table)
                truncate_table = "truncate table lineitem_q9"
                self.cur.execute(truncate_table)
                truncate_table = "truncate table orders_q9"
                self.cur.execute(truncate_table)
                self.con.commit()
                self.RS_cost_time.append(end-start)
            else:
                continue
    
    def GetQueryResult(self, truth_file_path):
        truth_file = open(truth_file_path, 'r')
        for line in truth_file.readlines()[1:-1]:
            elements = line.strip().split('|')
            self.timestamp_list.append(float(elements[0]))
            self.query_result.append(float(elements[1]))
        truth_file.close()
    
    def GetNoisedResult(self):
        self.noised_result = []
        for index in range(len(self.timestamp_list)):
            self.noised_result.append(self.query_result[index] + \
                self.RS[index] * 10 / self.epsilon * CauNoise())
    
    def WriteResult(self, answer_file_path, rounds, time_interval):
        answer_file = open(answer_file_path, 'w')
        writeline = "dataset=%s, query_name=%s, epsilon=%s, beta=%s, rounds=%s (RS)"%(dataset, query_name, epsilon, self.beta, rounds) + '\n'
        answer_file.write(writeline)
        for index in range(len(self.timestamp_list)):
            noise = self.noised_result[index] - self.query_result[index]
            writeline = str(self.timestamp_list[index]) + '|' + str(self.noised_result[index]) + '|' + str(self.RS_cost_time[index])\
                    + '|' + str(self.query_result[index]) + '|' + str(noise)
            answer_file.write(writeline + '\n')
        answer_file.write('Time=%ss, RS=%s, RS cost time=%s'%(time_interval, self.RS, self.RS_cost_time))
        answer_file.close()


def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = ""
    global relation_nums
    relation_nums = 1
    input_file_path = ""
    answer_files_prefix = ""
    query = ""
    # Privacy budget
    global epsilon
    epsilon = 4
    # Delta parameter:
    Delta = 10 ** -10
    # Experiments rounds
    global rounds
    rounds = 1
    global psql_user
    psql_user = "postgres"
    global psql_password
    psql_password = "postgres"
    global psql_host
    psql_host = '/tmp/'
    global psql_port
    psql_port = 5432

    try:
        opts, args = getopt.getopt(argv,"d:q:e:D:b:r:u:p:h:o:help:",["dataset=","query_name=", "epsilon=", "Delta=", "rounds=", "psql_user=", "psql_password=", "psql_host=", "psql_port", "help="])
    except getopt.GetoptError:
        print("TPCHq9RS.py -d <dataset> -q <query name> -e <epsilon(default 1)> -D <Delta (default 10^-10)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")> -o <psql_port(default 5432)>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-help':
            print("TPCHq9RS.py -d <dataset> -q <query name> -e <epsilon(default 1)> -D <Delta (default 10^-10)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")> -o <psql_port(default 5432)>")
            sys.exit()
        elif opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
            query_file = open('../Query/' + query_name + ".txt", 'r')
            query = ""
            for line in query_file.readlines():
                query += str(line)
            if query_name in ("four_star", "rectangle", "q7", "q9"):
                relation_nums = 4
            else:
                print("query_name in wrong domain, please make sure -q being correct")
                sys.exit()
        elif opt in ("-e","--epsilon"):
            epsilon = float(arg)
        elif opt in ("-D","--Delta"):
            Delta = float(arg)
        elif opt in ("-r", "--rounds"):
            rounds = int(arg)
        elif opt in ("-u", "--psql_user"):
            psql_user = str(arg)
        elif opt in ("-p", "--psql_password"):
            psql_password = str(arg)
        elif opt in ("-h", "--psql_host"):
            psql_host = str(arg)
        elif opt in ("-o", "--psql_port"):
            psql_port = int(arg)
    currentTime = str(datetime.today().date()) + '-' + datetime.now().strftime("%H-%M-%S")
    answer_files_prefix = "../Temp/" + dataset + '/answer/'
    return epsilon, Delta, answer_files_prefix, currentTime

def main(argv):
    start = time.time()
    footstep = 500000
    epsilon, Delta, answer_files_prefix, currentTime = loaddata(sys.argv[1:])

    relation_list = ['supplier', 'partsupp', 'lineitem', 'orders']
    file_list = []
    for relation in relation_list:
        file = open('../Temp/TPCH/' + relation + '.tbl', 'r')
        file_list.append(file)

    length_list = []
    for file in file_list:
        length_list.append(len(file.readlines()))
        file.seek(0)
    T = sum(length_list)
    random_list = ['PS' for _ in range(length_list[1])] + ['L' for _ in range(length_list[2])] + ['O' for _ in range(length_list[3])] 
    random.shuffle(random_list)
    random_list = ['S' for _ in range(length_list[0])] + random_list

    # Step 1: divide total privacy budget to per timestamp
    epsilon = epsilon / (2 * math.sqrt(2*T*math.log(1/Delta)))

    # Step 2: get beta used in Residual Sensitivity
    #         this beta is NOT the failure probability
    beta = epsilon / 10
    CGRS = TPCHq9RS(relation_list, epsilon, beta, T, query_name)

    # Step 3: get Residual Sensitivity with fixed footstep
    CGRS.GetResidualSensitivity(footstep, random_list)

    # Get query result from non-private setting result for efficient
    truth_file_prefix = "../Experiment/" + dataset + "/Truth/" + query_name
    truth_file_path = truth_file_prefix + '/' + os.listdir(truth_file_prefix)[0]
    CGRS.GetQueryResult(truth_file_path)

    # Step 4: get noised results, write them into the answer file
    end = time.time()
    answer_file_path = answer_files_prefix + query_name + '_RS_' + str(rounds) + '_' + currentTime + '.txt'
    CGRS.GetNoisedResult()
    CGRS.WriteResult(answer_file_path, rounds, end-start)
    
    CGRS.DropTables()
    print("RS baseline on dataset: %s with query: %s is finished, cost: %ss"%(dataset, query_name, end-start))

if __name__ == '__main__':
    main(sys.argv[1:])