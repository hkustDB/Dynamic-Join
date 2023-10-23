import getopt
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
class TPCHq7Truth:
    def __init__(self, relation_list, T, query_name, query=""):
        # Adjust parameter to optimize the error
        self.start_prefix = 9
        self.multiplier = 2
        self.ratio = 0.8

        # Initialize the relation instance used in the temporal experimental:
        self.relation_list = relation_list
        self.relations_instance = self.Relations_Instance(query_name)
        self.delta_relations_instance = self.Delta_Relations_Instance(query_name)

        # Initialize the parameter
        self.query_name = query_name
        self.query = self.Query_Instance(query)
        
        # Initialize the other parameter
        self.t = 0

        # Initialize the parameters for the first SVT corresponding to
        # Initialize ClipDP
        # self.ClipDP = ClipDP(self.epsilon/self.m_max, self.tau, self.T, self.query_name)
        self.con, self.cur = self.ConnectPsql()
        self.CreateTables()

    def Relations_Instance(self, query_name):
        relations_instance = []
        for relation in self.relation_list:
            relations_instance.append("%s_truth_%s_%s"%(query_name, rounds, relation))
        return relations_instance
    
    def Delta_Relations_Instance(self, query_name):
        delta_relations_instance = []
        for relation in self.relation_list:
            delta_relations_instance.append("%s_truth_%s_delta_%s"%(query_name, rounds, relation))
        return delta_relations_instance

    def Query_Instance(self, query):
        query_instance = query
        for num in range(1, relation_nums+1):
            query_instance = query_instance.replace(self.relation_list[num-1], self.relations_instance[num-1])
        return query_instance


    def Update(self, update_tuple):
        self.t += 1
        relation_label = update_tuple[0]
        relation_tuple = update_tuple[1:]
        return self.t, relation_tuple
    
    
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
    input_file_path = ""
    intermediate_files_prefix = ""
    answer_file_path = ""
    query = ""
    # Privacy budget
    epsilon = 4
    # Error probability: with probability at at least 1-beta, the error can be bounded
    beta = 0.1
    theta = 1
    global tau
    tau = 16
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
        opts, args = getopt.getopt(argv,"d:q:r:u:p:h:o:help:",["dataset=","query_name=", "rounds=", "psql_user=", "psql_password=", "psql_host=", "psql_port", "help="])
    except getopt.GetoptError:
        print("TPCHq7Truth.py -d <dataset> -q <query name> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-help':
            print("TPCHq7Truth.py -d <dataset> -q <query name> -r <rounds(default 1)> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
            sys.exit()
        elif opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
            query_file = open('../Query/' + query_name + ".txt", 'r')
            query = ""
            for line in query_file.readlines():
                query += str(line)
            if query_name in ("q7", "q9"):
                relation_nums = 4
            else:
                print("query_name in wrong domain, please make sure -q being correct")
                sys.exit()
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
    answer_file_path = "../Temp/" + dataset + '/answer/' + query_name + '_truth_' + str(rounds) + '_' + currentTime + '.txt'
    return query_name, query, answer_file_path


def main(argv):
    start = time.time()
    footsteps = 500000
    query_name, query, answer_file_path = loaddata(sys.argv[1:])

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
        intermediate_file_path = '../Temp/TPCH/intermediate/q7_' + relation + '_truth_' + str(rounds) + '.txt'
        intermediate_file_path_list.append(intermediate_file_path)
        intermediate_file_list.append(open(intermediate_file_path, 'w+'))

    answer_file = open(answer_file_path, 'w')
    
    TPCH = TPCHq7Truth(relation_list, T, query_name, query)
    writeline = "dataset=TPCH, query_name=q7, (Truth)" + '\n'
    answer_file.write(writeline)
    
    res = 0
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

        t, temp_tuple = TPCH.Update(update_tuple)

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
            time_length = finish_timestamp - start_timestamp
            writeline = str(t) + '|' + str(res) + '|' + str(time_length)
            answer_file.write(writeline + '\n')
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