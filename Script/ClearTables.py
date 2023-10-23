import getopt
import sys
import psycopg2
import shutil
import os

# The Script drops all the tables in the corresponding database but keeps the empty database
def loaddata(argv):
    global datasets
    datasets = ""
    global psql_user
    psql_user = "postgres"
    global psql_password
    psql_password = "postgres"
    global psql_host
    psql_host = None
    global psql_port
    psql_port = 5432
    
    try:
        opts, args = getopt.getopt(argv, "d:u:p:h:o:", ["dataset=", "psql_user=", "psql_password=", "psql_host=", "psql_port="])
    except getopt.GetoptError:
        print("ClearTables.py -d <dataset> -u <psql_user> -p <psql_password> -h <psql_host>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "--dataset"):
            datasets = list(str(arg).replace(' ', '').split(','))
        elif opt in ("-u", "--psql_user"):
            psql_user = str(arg)
        elif opt in ("-p", "--psql_password"):
            psql_password = str(arg)
        elif opt in ("-h", "--psql_host"):
            psql_host = str(arg)
        elif opt in ("-o", "--psql_port"):
            psql_port = int(arg)
    return datasets

def CutPsqlConnection(datasets):
    for dataset in datasets:
        con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        cut_connection = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '%s' AND pid <> pg_backend_pid();"%dataset
        cur.execute(cut_connection)
        con.commit()
        con.close()

def RemoveDatasets(datasets):
    for dataset in datasets:
        con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
        cur = con.cursor()
        drop_tables = "drop SCHEMA public CASCADE;CREATE SCHEMA public;"
        cur.execute(drop_tables)
        con.commit()
        con.close()

def RemoveIntermediateFiles(datasets):
    for dataset in datasets:
        path = '../Temp/' + dataset + '/intermediate'
        shutil.rmtree(path)
        os.mkdir(path)

def RemoveAnswerFiles(datasets):
    for dataset in datasets:
        path = '../Temp/' + dataset + '/answer/FixTau'
        if os.path.isdir(path):
            listdir_1 = os.listdir(path)
            for dirname_1 in listdir_1:
                dirname_1 = path + '/' + dirname_1
                if os.path.isdir(dirname_1):
                    list_dir_2 = os.listdir(dirname_1)
                    for dirname_2 in list_dir_2:
                        dirname_2 = dirname_1 + '/' + dirname_2
                        if os.path.isdir(dirname_2):
                            shutil.rmtree(dirname_2)
                            os.mkdir(dirname_2)
    for dataset in datasets:
        path = '../Temp/' + dataset + '/answer/DP'
        if os.path.isdir(path):
            listdir_1 = os.listdir(path)
            for dirname_1 in listdir_1:
                dirname_1 = path + '/' + dirname_1
                if os.path.isdir(dirname_1):
                    shutil.rmtree(dirname_1)
                    os.mkdir(dirname_1)
    for dataset in datasets:
        path = '../Temp/' + dataset + '/answer/RS'
        if os.path.isdir(path):
            listdir_1 = os.listdir(path)
            for dirname_1 in listdir_1:
                dirname_1 = path + '/' + dirname_1
                if os.path.isdir(dirname_1):
                    shutil.rmtree(dirname_1)
                    os.mkdir(dirname_1)
    for dataset in datasets:
        path = '../Temp/' + dataset + '/answer/Truth'
        if os.path.isdir(path):
            listdir_1 = os.listdir(path)
            for dirname_1 in listdir_1:
                dirname_1 = path + '/' + dirname_1
                if os.path.isdir(dirname_1):
                    shutil.rmtree(dirname_1)
                    os.mkdir(dirname_1)


def main(argv):
    global relation_nums
    relation_nums = 2
    datasets = loaddata(sys.argv[1:])
    CutPsqlConnection(datasets)
    RemoveDatasets(datasets)
    RemoveIntermediateFiles(datasets)
    RemoveAnswerFiles(datasets)
    print('%s is(are) cleared for experiments'%datasets)

if __name__ == '__main__':
    main(sys.argv[1:])