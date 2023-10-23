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
import io
import subprocess

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

class ConGraphRS:
    def __init__(self, epsilon, beta, T, query_name, query=""):
        self.epsilon = epsilon
        self.beta = beta
        self.T = T
        self.query_name = query_name
        self.query = self.Query_Instance(query)

        self.RS_cost_time = []
        self.RS = []
        self.timestamp_list= []
        self.query_result = []
        self.noised_result = []

        self.con, self.cur = self.ConnectPsql()
        self.CreateTables()

    def Query_Instance(self, query):
        query_instance = query
        for num in range(1, relation_nums+1):
            query_instance.replace("R%s"%num, "edge_%s"%query_name)
        return query_instance

    def ConnectPsql(self):
        con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        return con, cur

    def CreateTables(self):
        code = "create table edge_%s (from_id integer not null, to_id integer not null);"%query_name
        self.cur.execute(code)
        self.con.commit()

    def DropTables(self):
        code = "drop table edge_%s;"%query_name
        self.cur.execute(code)
        self.con.commit()

    # Use ResidualSensitivity.py to compute the RS
    def GetResidualSensitivity(self, footstep):
        for t in tqdm(range(1, self.T+1)):
            if t % footstep == 0:
                with open('../Temp/%s/%s.csv'%(dataset, dataset), 'r') as f:
                    lines = f.readlines()[:t]
                    data = ''.join(lines)
                    self.cur.copy_from(io.StringIO(data), 'edge_%s'%query_name, sep='|')
                self.con.commit()
                start = time.time()
                args = ['../Code/ResidualSensitivity.py', '-d', dataset, '-q', query_name, '-b', '%s'%self.beta, '-u', '%s'%psql_user, '-p', '%s'%psql_password, '-h', '%s'%psql_host, '-o', '%s'%psql_port]
                output = subprocess.check_output(['python'] + args)
                result = float(output.decode().strip())
                self.RS.append(result)
                end = time.time()
                code = "truncate table edge_%s;"%query_name
                self.cur.execute(code)
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
        print("ConGraphRS.py -d <dataset> -q <query name> -e <epsilon(default 1)> -D <Delta (default 0.1)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-help':
            print("ConGraphRS.py -d <dataset> -q <query name> -e <epsilon(default 1)> -D <Delta (default 0.1)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
            sys.exit()
        elif opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
            query_file = open('../Query/' + query_name + ".txt", 'r')
            query = ""
            for line in query_file.readlines():
                query += str(line)
            if query_name in ("two_star", "two_path"):
                relation_nums = 2
            elif query_name in ("three_star", "three_path", "triangle"):
                relation_nums = 3
            elif query_name in ("four_star", "rectangle"):
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
    input_file_path = "../Temp/" + dataset + '/' + dataset + '.csv'
    answer_files_prefix = "../Temp/" + dataset + '/answer/'
    return epsilon, Delta, input_file_path, answer_files_prefix, currentTime

def main(argv):
    start = time.time()
    footstep = 500000
    epsilon, Delta, input_file_path, answer_files_prefix, currentTime = loaddata(sys.argv[1:])
    input_file = open(input_file_path, 'r')

    # Finite Time length: T
    T = len(input_file.readlines())
    input_file.close()

    # Step 1: divide total privacy budget to per timestamp
    epsilon = epsilon / (2 * math.sqrt(2*T*math.log(1/Delta)))

    # Step 2: get beta used in Residual Sensitivity
    #         this beta is NOT the failure probability
    beta = epsilon / 10
    CGRS = ConGraphRS(epsilon, beta, T, query_name)

    # Step 3: get Residual Sensitivity with fixed footstep
    CGRS.GetResidualSensitivity(footstep)

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