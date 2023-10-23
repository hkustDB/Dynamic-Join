import getopt
import sys
import GlobalSensitivity as GlobalSensitivity
import numpy as np
import time
import os
import psycopg2
from datetime import datetime
from tqdm import tqdm

class ConGraphTruth:
    def __init__(self, T, query_name, query):
        self.relations_instance = self.Relations_Instance(query_name)
        self.delta_relations_instance = self.Delta_Relations_Instance(query_name)

        # Initialize the parameter
        self.query_name = query_name
        self.query = self.Query_Instance(query)

        # Initialize the other parameter
        self.t = 0
        
        # Initialize the connection
        self.con, self.cur = self.ConnectPsql()
        self.CreateTables()

    def Relations_Instance(self, query_name):
        relations_instance = []
        for num in range(1, relation_nums+1):
            relations_instance.append("%s_truth_%s_r%s"%(query_name, rounds, num))
        return relations_instance
    
    def Delta_Relations_Instance(self, query_name):
        delta_relations_instance = []
        for num in range(1, relation_nums+1):
            delta_relations_instance.append("%s_truth_%s_delta_r%s"%(query_name, rounds, num))
        return delta_relations_instance
    
    def Query_Instance(self, query):
        query_instance = query
        for num in range(1, relation_nums+1):
            query_instance = query_instance.replace("R%s"%num, self.relations_instance[num-1])
        return query_instance
    
    def ConnectPsql(self):
        con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        return con, cur

    def CreateTables(self):
        for num in range(1, relation_nums+1):
            create_temp_table = "create table %s (timestamp integer not null, from_id integer not null, to_id integer not null)"%self.relations_instance[num-1]
            self.cur.execute(create_temp_table)
            create_delta_table = "create table %s (timestamp integer not null, from_id integer not null, to_id integer not null)"%self.delta_relations_instance[num-1]
            self.cur.execute(create_delta_table)
            self.con.commit()

    def Update(self, from_id, to_id):
        self.t += 1
        temp_tuples = [(from_id, to_id)] * relation_nums
        return self.t, temp_tuples

    def CopyDeltaData(self, intermediate_files=None, tuples=None, type=""):
        if type == "tuple":
            for num in range(1, relation_nums+1):
                if tuples[num-1] == None:
                    continue
                else:
                    insert_delta = "insert into %s values (%s, %s, %s)"%(self.delta_relations_instance[num-1], self.t, tuples[num-1][0], tuples[num-1][1])
                    self.cur.execute(insert_delta)
                    self.con.commit()
        elif type == "file":
            for num in range(1, relation_nums+1):
                intermediate_files[num-1].seek(0)
                self.cur.copy_from(intermediate_files[num-1], "%s"%self.delta_relations_instance[num-1], sep='|')
                self.con.commit()
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
            insert_delta = "insert into %s (timestamp, from_id, to_id) select timestamp, from_id, to_id from %s"%(self.relations_instance[num-1], self.delta_relations_instance[num-1])
            self.cur.execute(insert_delta)
            truncate_relation = "truncate table %s"%self.delta_relations_instance[num-1]
            self.cur.execute(truncate_relation)
            self.con.commit()
        return delta_Q
    
    def RemoveTables(self):
        for num in range(1, relation_nums+1):
            remove_relation = "drop table %s"%self.relations_instance[num-1]
            self.cur.execute(remove_relation)
            remove_delta_relation = "drop table %s"%self.delta_relations_instance[num-1]
            self.cur.execute(remove_delta_relation)
            self.con.commit()
        self.con.close()

def loaddata(argv):
    global dataset
    dataset = ""
    global query_name
    query_name = ""
    global relation_nums
    relation_nums = 1
    input_file_path = ""
    intermediate_files_prefix = ""
    answer_file_path = ""
    query = ""
    # Experiment round
    global rounds
    rounds = 0
    global psql_user
    psql_user = "postgres"
    global psql_password
    psql_password = "postgres"
    global psql_host
    psql_host = None
    global psql_port
    psql_port = 5432

    try:
        opts, args = getopt.getopt(argv,"d:q:r:u:p:h:o:",["dataset=", "query_name=", "rounds=", 'psql_user=', 'psql_password=', 'psql_host=', 'psql_port='])
    except getopt.GetoptError:
        print("ConGraphTruth.py -d <dataset> -q <query name>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--data_name"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
            query_file = open('../Query/' + query_name + ".txt", 'r')
            query = ""
            for line in query_file.readlines():
                query += str(line)
            if query_name in ("two_path"):
                relation_nums = 2
            elif query_name in ("three_path", "triangle"):
                relation_nums = 3
            elif query_name in ("rectangle"):
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
    input_file_path = "../Temp/" + dataset + '/' + dataset + '.csv'
    intermediate_files_prefix = "../Temp/" + dataset + '/intermediate/' + query_name + '_truth_' + str(rounds) + '_'
    answer_file_path = "../Temp/" + dataset + '/answer/' + query_name + '_truth_' + str(rounds) +'_' + currentTime + '.txt'
    return query_name, query, input_file_path, intermediate_files_prefix, answer_file_path

def main(argv):
    start = time.time()
    footsteps = 500000
    query_name, query, input_file_path, intermediate_files_prefix, answer_file_path = loaddata(sys.argv[1:])
    
    # Open input file, intermediate files, and answer file
    input_file = open(input_file_path, 'r')

    intermediate_files = []
    intermediate_files_path = []
    for num in range(1, relation_nums+1):
        intermediate_file_path = intermediate_files_prefix + str(num) + '.txt'
        intermediate_files_path.append(intermediate_file_path)
        intermediate_files.append(open(intermediate_file_path, 'w+'))

    answer_file = open(answer_file_path, 'w')
    writeline = "dataset=%s, query_name=%s (Truth)"%(dataset, query_name) + '\n'
    answer_file.write(writeline)
    
    T = len(input_file.readlines())
    CGT = ConGraphTruth(T, query_name, query)
    res = 0
    input_file.seek(0)
    for line in tqdm(input_file.readlines()):
        elements = line.strip().split('|')
        from_id = int(elements[0])
        to_id = int(elements[1])
        t, temp_tuples = CGT.Update(from_id, to_id)

        if t % footsteps == 0:
            CGT.CopyDeltaData(intermediate_files=intermediate_files, type="file")
            res += CGT.DeltaQuery()
            start_timestamp = time.time()
            CGT.CopyDeltaData(tuples=temp_tuples, type="tuple")
            res += CGT.DeltaQuery()
            finish_timestamp = time.time()
            time_length = finish_timestamp - start_timestamp
            writeline = str(t) + '|' + str(res) + '|' + str(time_length)
            answer_file.write(writeline + '\n')
        else:
            for num in range(1, relation_nums+1):
                if temp_tuples[num-1] == None:
                    continue
                else:
                    writeline = str(t) + '|' + str(temp_tuples[num-1][0]) + '|' + str(temp_tuples[num-1][1])
                    intermediate_files[num-1].write(str(writeline) + '\n')
    CGT.RemoveTables()
    input_file.close()
    # Close and remove the intermediate_files
    for num in range(1, relation_nums+1):
        intermediate_files[num-1].close()
        os.remove(intermediate_files_path[num-1])
    end = time.time()
    writeline = "Time=" + str(end - start) + 's'
    answer_file.write(writeline + '\n')
    answer_file.close()

if __name__ == '__main__':
    main(sys.argv[1:])