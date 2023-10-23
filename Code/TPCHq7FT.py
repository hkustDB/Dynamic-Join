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

# This file is used to return the results of ConGraphDP mechanism queries at specific time intervals.

random.seed(13)
def LapNoise():
    a = random.uniform(0, 1)
    b = math.log(1/(1-a))
    c = random.uniform(0, 1)
    if c > 0.5:
        return b
    else:
        return -b
    
class TPCHq7FixTau:
    def __init__(self, relation_list, epsilon, beta, T, query_name, query=""):
        # Initialize the relation instance used in the temporal experimental:
        self.relation_list = relation_list
        self.relations_instance = self.Relations_Instance(epsilon, query_name)
        self.delta_relations_instance = self.Delta_Relations_Instance(epsilon, query_name)

        # Initialize the parameter
        self.epsilon = epsilon
        self.beta = beta
        self.T = T
        self.query_name = query_name
        self.query = self.Query_Instance(query)
        
        # Initialize the other parameter
        self.t = 0
        self.last_t = 0

        # Initialize the parameters for the first SVT corresponding to
        self.k_C = 1
        self.boundaries = self.Boundaries()
        self.tau = {}
        self.tuples_degree = {}
        self.tuples_clipped_tuples = {}
        self.k_S = {}
        self.beta_S = {}
        self.ClipNum = {}
        for boundary in self.boundaries:
            self.tau[boundary] = tau
            self.tuples_degree[boundary] = {}
            self.tuples_clipped_tuples[boundary] = {}
            self.ClipNum[boundary] = 0
        self.tau[('C', 'ck')] = 1
        self.tau[('O', 'ck')] = 1
        self.tau[('S', 'sk')] = 1
        # Initialize ClipDP
        # self.ClipDP = ClipDP(self.epsilon/self.m_max, self.tau, self.T, self.query_name)
        self.bias = 0
        self.real_count = 0
        self.con, self.cur = self.ConnectPsql()
        self.CreateTables()

    def Relations_Instance(self, epsilon, query_name):
        relations_instance = []
        for relation in self.relation_list:
            relations_instance.append("%s_fixtau_%s_%s_%s"%(query_name, tau, rounds, relation))
        return relations_instance
    
    def Delta_Relations_Instance(self, epsilon, query_name):
        delta_relations_instance = []
        for relation in self.relation_list:
            delta_relations_instance.append("%s_fixtau_%s_%s_delta_%s"%(query_name, tau, rounds, relation))
        return delta_relations_instance

    def Query_Instance(self, query):
        query_instance = query
        for num in range(1, relation_nums+1):
            query_instance = query_instance.replace(self.relation_list[num-1], self.relations_instance[num-1])
        return query_instance

    def UpdateDegree(self, relation_label, relation_tuple):
        if relation_label == "C":
            if (relation_tuple[0]) in self.tuples_degree[('C','ck')].keys():
                self.tuples_degree[('C','ck')][(relation_tuple[0])] += 1
            else:
                self.tuples_degree[('C', 'ck')][(relation_tuple[0])] = 1
        elif relation_label == "O":
            if (relation_tuple[0]) in self.tuples_degree[('O','ck')].keys():
                self.tuples_degree[('O','ck')][(relation_tuple[0])] += 1
            else:
                self.tuples_degree[('O', 'ck')][(relation_tuple[0])] = 1
            if (relation_tuple[1]) in self.tuples_degree[('O','ok')].keys():
                self.tuples_degree[('O','ok')][(relation_tuple[1])] += 1
            else:
                self.tuples_degree[('O', 'ok')][(relation_tuple[1])] = 1
        elif relation_label == "L":
            if (relation_tuple[0]) in self.tuples_degree[('L','ok')].keys():
                self.tuples_degree[('L','ok')][(relation_tuple[0])] += 1
            else:
                self.tuples_degree[('L', 'ok')][(relation_tuple[0])] = 1
            if (relation_tuple[1]) in self.tuples_degree[('L','sk')].keys():
                self.tuples_degree[('L', 'sk')][(relation_tuple[1])] += 1
            else:
                self.tuples_degree[('L', 'sk')][(relation_tuple[1])] = 1
        elif relation_label == "S":
            if (relation_tuple[0]) in self.tuples_degree[('S','sk')].keys():
                self.tuples_degree[('S','sk')][(relation_tuple[0])] += 1
            else:
                self.tuples_degree[('S', 'sk')][(relation_tuple[0])] = 1

    def Graph_Clip_Tuples(self, relation_label, relation_tuple):
        if relation_label == 'C':
            return relation_tuple
        elif relation_label == 'O':
            if self.tuples_degree[('O', 'ck')][(relation_tuple[0])] < self.tau[('O', 'ck')]:
                return relation_tuple
            return None
        elif relation_label == 'L':
            if self.tuples_degree[('L', 'ok')][(relation_tuple[0])] < self.tau[('L', 'ok')] and self.tuples_degree[('L', 'sk')][(relation_tuple[1])] < self.tau[('L', 'sk')]:
                return relation_tuple
            return None
        elif relation_label == 'S':
            return relation_tuple

    def Update(self, update_tuple):
        relation_label = update_tuple[0]
        relation_tuple = update_tuple[1:]
        self.t += 1
        self.UpdateDegree(relation_label, relation_tuple)
        temp_tuple = self.Graph_Clip_Tuples(relation_label, relation_tuple)
        return self.t, temp_tuple

    # Get boundaries
    # e.g. triangle query:
    # (1, from), (1, to), (2, from), (2, to), (3, from), (3, to)
    def Boundaries(self):
        boundaries = [('C', 'ck'), ('O', 'ck'), ('O', 'ok'), ('L', 'ok'), ('L', 'sk'), ('S', 'sk')]
        return boundaries
    
    def ConnectPsql(self):
        con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        return con, cur
    
    def CreateTables(self):
        create_temp_table = "create table %s (timestamp integer not null, ck integer not null)"%self.relations_instance[0]
        self.cur.execute(create_temp_table)
        create_temp_table = "create table %s (timestamp integer not null, ck integer not null, ok integer not null)"%self.relations_instance[1]
        self.cur.execute(create_temp_table)
        create_temp_table = "create table %s (timestamp integer not null, ok integer not null, sk integer not null)"%self.relations_instance[2]
        self.cur.execute(create_temp_table)
        create_temp_table = "create table %s (timestamp integer not null, sk integer not null)"%self.relations_instance[3]
        self.cur.execute(create_temp_table)
        create_delta_table = "create table %s (timestamp integer not null, ck integer not null)"%self.delta_relations_instance[0]
        self.cur.execute(create_delta_table)
        create_delta_table = "create table %s (timestamp integer not null, ck integer not null, ok integer not null)"%self.delta_relations_instance[1]
        self.cur.execute(create_delta_table)
        create_delta_table = "create table %s (timestamp integer not null, ok integer not null, sk integer not null)"%self.delta_relations_instance[2]
        self.cur.execute(create_delta_table)
        create_delta_table = "create table %s (timestamp integer not null, sk integer not null)"%self.delta_relations_instance[3]
        self.cur.execute(create_delta_table)
        self.con.commit()

    def CopyDeltaData(self, relation_label, intermediate_files=None, tuple=None, type=""):
        if type == "tuple":
            if tuple == None:
                return
            else:
                if relation_label == 'C':
                    insert_delta = "insert into %s values (%s, %s)"%(self.delta_relations_instance[0], self.t, tuple[0])
                    self.cur.execute(insert_delta)
                    self.con.commit()
                elif relation_label == 'O':
                    insert_delta = "insert into %s values (%s, %s, %s)"%(self.delta_relations_instance[1], self.t, tuple[0], tuple[1])
                    self.cur.execute(insert_delta)
                    self.con.commit()
                elif relation_label == 'L':
                    insert_delta = "insert into %s values (%s, %s, %s)"%(self.delta_relations_instance[2], self.t, tuple[0], tuple[1])
                    self.cur.execute(insert_delta)
                    self.con.commit()
                elif relation_label == 'S':
                    insert_delta = "insert into %s values (%s, %s)"%(self.delta_relations_instance[3], self.t, tuple[0])
                    self.cur.execute(insert_delta)
                    self.con.commit()
        elif type == "file":
            intermediate_files[0].seek(0)
            self.cur.copy_from(intermediate_files[0], "%s"%self.delta_relations_instance[0], sep='|')
            intermediate_files[1].seek(0)
            self.cur.copy_from(intermediate_files[1], "%s"%self.delta_relations_instance[1], sep='|')
            intermediate_files[2].seek(0)
            self.cur.copy_from(intermediate_files[2], "%s"%self.delta_relations_instance[2], sep='|')
            intermediate_files[3].seek(0)
            self.cur.copy_from(intermediate_files[3], "%s"%self.delta_relations_instance[3], sep='|')
            for num in range(1, relation_nums+1):
                intermediate_files[num-1].seek(0)
                intermediate_files[num-1].truncate()

    def DeltaQuery(self):
        delta_query = self.query
        delta_Q = 0
        for num in range(1, relation_nums+1):
            delta_query = self.query.replace(self.relations_instance[num-1], self.delta_relations_instance[num-1])
            self.cur.execute(delta_query)
            delta_Q += self.cur.fetchone()[0]
            # Copy current tuple to the original relation
            insert_delta = "insert into %s select * from %s"%(self.relations_instance[num-1], self.delta_relations_instance[num-1])
            self.cur.execute(insert_delta)
            truncate_relation = "truncate table %s"%self.delta_relations_instance[num-1]
            self.cur.execute(truncate_relation)
            self.con.commit()
        return delta_Q
    
    def Noise(self):
        noise = 0
        simulated_T = self.T
        simulated_t = self.t
        epsilon = self.epsilon
        while simulated_t != 0:
            simulated_t = simulated_t >> 1
            if simulated_t%2 == 1:
                noise += LapNoise() * 1/epsilon * math.log(simulated_T+1, 2) * self.GS_Q()
            else:
                continue
        writeline = str('GS_Q:%s'%self.GS_Q()) + str(' epsilon:%s'%(epsilon)) + str(' log factor:%s'%math.log(simulated_T+1, 2)) + str(' k_C:%s'%self.k_C)
        return noise, writeline
    
    def GS_Q(self):
        GS_method = "GS_Q_" + self.query_name
        GS_Q = getattr(GlobalSensitivity, GS_method)(self.tau)
        return GS_Q

    def RemoveTables(self):
        drop_temp_table = "drop table %s"%self.relations_instance[0]
        self.cur.execute(drop_temp_table)
        drop_temp_table = "drop table %s"%self.relations_instance[1]
        self.cur.execute(drop_temp_table)
        drop_temp_table = "drop table %s"%self.relations_instance[2]
        self.cur.execute(drop_temp_table)
        drop_temp_table = "drop table %s"%self.relations_instance[3]
        self.cur.execute(drop_temp_table)
        drop_delta_table = "drop table %s"%self.delta_relations_instance[0]
        self.cur.execute(drop_delta_table)
        drop_delta_table = "drop table %s"%self.delta_relations_instance[1]
        self.cur.execute(drop_delta_table)
        drop_delta_table = "drop table %s"%self.delta_relations_instance[2]
        self.cur.execute(drop_delta_table)
        drop_delta_table = "drop table %s"%self.delta_relations_instance[3]
        self.cur.execute(drop_delta_table)
        self.con.commit()
        self.con.close()

def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = "q7"
    global relation_nums
    relation_nums = 1
    answer_file_path = ""
    query = ""
    # Privacy budget
    epsilon = 4
    # Error probability: with probability at at least 1-beta, the error can be bounded
    beta = 0.1
    global tau
    tau = 2
    # Experiment round
    global rounds
    rounds = 1
    global psql_user
    psql_user = "postgres"
    global psql_password
    psql_password = "postgres"
    global psql_host
    psql_host = None
    global psql_port
    psql_port = 5432

    try:
        opts, args = getopt.getopt(argv,"d:q:e:b:t:T:r:u:p:h:o:help:",["dataset=","query_name=", "epsilon=","beta=", "tau=", "rounds=", "psql_user=", "psql_password=", "psql_host=", "psql_port", "help="])
    except getopt.GetoptError:
        print("TPCHq7FT.py -d <dataset> -q <query name> -e <epsilon(default 1)> -b <beta(default 0.1)> -T <tau(default 16)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-help':
            print("TPChq7FT.py -d <dataset> -q <query name> -e <epsilon(default 1)> -b <beta(default 0.1)> -T <tau(default 16)> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
            sys.exit()
        elif opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
            query_file = open('../Query/' + query_name + ".txt", 'r')
            query = ""
            for line in query_file.readlines():
                query += str(line)
            if query_name in ("q7"):
                relation_nums = 4
            else:
                print("query_name in wrong domain, please make sure -q being correct")
                sys.exit()
        elif opt in ("-e","--epsilon"):
            epsilon = float(arg)
        elif opt in ("-b","--beta"):
            beta = float(arg)
        elif opt in ("-T", "--tau"):
            tau = int(arg)
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
    answer_file_path = "../Temp/" + dataset + '/answer/' + 'q7_FixTau_' + str(tau) + '_' + str(rounds) + '_' + currentTime + '.txt'
    return epsilon, beta, query_name, query, answer_file_path


def main(argv):
    start = time.time()
    footsteps = 500000
    epsilon, beta, query_name, query, answer_file_path = loaddata(sys.argv[1:])

    relation_list = ['customer', 'orders', 'lineitem', 'supplier']
    file_list = []
    for relation in relation_list:
        file = open('../Temp/TPCH/' + relation + '.tbl', 'r')
        file_list.append(file)
    
    length_list = []
    for file in file_list:
        length_list.append(len(file.readlines()))
        file.seek(0)
    T = sum(length_list)
    random_list = ['O' for _ in range(length_list[1])] + ['L' for _ in range(length_list[2])]
    random.shuffle(random_list)
    random_list = ['C' for _ in range(length_list[0])] + ['S' for _ in range(length_list[3])] + random_list


    intermediate_file_path_list = []
    intermediate_file_list = []
    for relation in relation_list:
        intermediate_file_path = '../Temp/TPCH/intermediate/q7_' + relation + '_FixTau_' + str(tau) + '_' + str(rounds) + '.txt'
        intermediate_file_path_list.append(intermediate_file_path)
        intermediate_file_list.append(open(intermediate_file_path, 'w+'))

    answer_file = open(answer_file_path, 'w')
    
    TPCH = TPCHq7FixTau(relation_list, epsilon, beta, T, query_name, query)
    writeline = "dataset=TPCH, query_name=q7, epsilon=%s, beta=%s, tau=%s, rounds=%s, (Intervals)"%(epsilon, beta, tau, rounds) + '\n'
    answer_file.write(writeline)
    
    res = 0
    res_noise = 0
    clipped_count = 0
    for t in tqdm(range(T)):
        relation_label = random_list[t]
        update_tuple = ()
        if relation_label == 'C':
            line = file_list[0].readline()
            elements = line.strip().split('|')
            update_tuple = ('C', elements[0])
        elif relation_label == 'O':
            line = file_list[1].readline()
            elements = line.strip().split('|')
            update_tuple = ('O', elements[1], elements[0])
        elif relation_label == 'L':
            line = file_list[2].readline()
            elements = line.strip().split('|')
            update_tuple = ('L', elements[0], elements[2])
        elif relation_label == 'S':
            line = file_list[3].readline()
            elements = line.strip().split('|')
            update_tuple = ('S', elements[0])

        t, temp_tuple, = TPCH.Update(update_tuple)

        if t % footsteps == 0:
            # When we are going to give the query result after footsteps:
            # 1. compute the increment caused by the delta_R: Q1
            # 2. compute the increment caused by the current insert tuple: temp_tuples: Q2
            # 3. sum the previous result, Q1, Q2, and noise:
            TPCH.CopyDeltaData(relation_label, intermediate_files=intermediate_file_list, type="file")
            res += TPCH.DeltaQuery()
            start_timestamp = time.time()
            TPCH.CopyDeltaData(relation_label, tuple=temp_tuple, type="tuple")
            res += TPCH.DeltaQuery()
            finish_timestamp = time.time()
            noise, writeline = TPCH.Noise()
            res_noise = res + noise
            time_length = finish_timestamp - start_timestamp
            writeline = str(t) + '|' + str(res_noise) + '|' + str(time_length)\
                    + '|' + str(res) + '|'  + str(noise) + '|' + writeline
            answer_file.write(writeline + '\n')
        else:
            if temp_tuple == None:
                clipped_count += 1
                continue
            else:
                if relation_label == 'C':
                    writeline = str(t) + '|' + str(temp_tuple[0])
                    intermediate_file_list[0].write(str(writeline) + '\n')
                elif relation_label == 'O':
                    writeline = str(t) + '|' + str(temp_tuple[0]) + '|' + str(temp_tuple[1])
                    intermediate_file_list[1].write(str(writeline) + '\n')
                elif relation_label == 'L':
                    writeline = str(t) + '|' + str(temp_tuple[0]) + '|' + str(temp_tuple[1])
                    intermediate_file_list[2].write(str(writeline) + '\n')
                elif relation_label == 'S':
                    writeline = str(t) + '|' + str(temp_tuple[0])
                    intermediate_file_list[3].write(str(writeline) + '\n')
    TPCH.RemoveTables()
    for file in file_list:
        file.close()
    # Close and remove the intermediate_files
    for intermediate_file in intermediate_file_list:
        intermediate_file.close()
    for intermediate_file_path in intermediate_file_path_list:
        os.remove(intermediate_file_path)
    end = time.time()
    writeline = "Time=" + str(end - start) + 's'
    answer_file.write(writeline + '\n')
    answer_file.close()
    print("Experiment on dataset: %s with query: %s is finished"%(dataset, query_name))

if __name__ == '__main__':
    main(sys.argv[1:])