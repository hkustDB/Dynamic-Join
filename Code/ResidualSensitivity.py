import psycopg2
import sys, getopt, math
import numpy as np

def CollectTEtriangle():
    global TE
    con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select r1.from_id, r2.to_id, count(*) from edge_%s as r1, edge_%s as r2 where r1.to_id = r2.from_id group by r1.from_id, r2.to_id) as t;"%(query_name, query_name)
    cur.execute(code)
    r1 = int(cur.fetchone()[0])
    TE = np.ones(8)
    TE[0b110] = r1
    TE[0b101] = r1
    TE[0b011] = r1
    con.commit()
    con.close()

def CollectTErectangle():
    global TE
    con = psycopg2.connect(database = dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select r1.from_id, r2.to_id, count(*) from edge_%s as r1, edge_%s as r2 where r1.to_id = r2.from_id group by r1.from_id, r2.to_id) as t;"%(query_name, query_name)
    cur.execute(code)
    r1 = int(cur.fetchone()[0])
    code = "create table t1 as (select r1.from_id, r2.to_id, count(*) from edge_%s as r1, edge_%s as r2 where r1.to_id = r2.from_id group by r1.from_id, r2.to_id);"%(query_name, query_name)
    cur.execute(code)
    code = "select max(sum) from (select t1.from_id, edge_%s.from_id, sum(count-1) from t1, edge_%s where t1.to_id = edge_%s.from_id group by t1.from_id, edge_%s.to_id) as t;"%(query_name, query_name, query_name, query_name)
    cur.execute(code)
    r2 = int(cur.fetchone()[0])
    code = "drop table t1;"
    cur.execute(code)
    TE = np.ones(16)
    TE[0b1100] = r1
    TE[0b1001] = r1
    TE[0b0110] = r1
    TE[0b0101] = r1
    TE[0b0011] = r1

    TE[0b1110] = r2
    TE[0b1101] = r2
    TE[0b1011] = r2
    TE[0b0111] = r2
    con.commit()
    con.close()
    
def CollectTEtwo_path():
    global TE
    con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select r1.to_id, count(*) as count from edge_%s as r1 group by r1.to_id) as t;"%query_name
    cur.execute(code)
    r1 = int(cur.fetchone()[0])
    code = "select max(count) from (select r1.from_id, count(*) from edge_%s as r1 group by r1.from_id) as t;"%query_name
    cur.execute(code)
    r2 = int(cur.fetchone()[0])
    TE = np.ones(4)
    TE[0b01] = r1
    TE[0b10] = r2
    con.commit()
    con.close()

def CollectTEthree_path():
    global TE
    con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select r1.from_id, count(*) from edge_%s as r1 group by r1.from_id) as t;"%query_name
    cur.execute(code)
    r1 = int(cur.fetchone()[0])
    code = "select max(count) from (select r1.to_id, count(*) from edge_%s as r1 group by r1.to_id) as t;"%query_name
    cur.execute(code)
    r4 = int(cur.fetchone()[0])
    code = "select max(count) from (select r1.from_id, count(*) from edge_%s as r1, edge_%s as r2 where r1.to_id = r2.from_id group by r1.from_id) as t;"%(query_name, query_name)
    cur.execute(code)
    r3 = int(cur.fetchone()[0])
    r5 = r1 * r4
    code = "select max(count) from (select r2.to_id, count(*) as count from edge_%s as r1, edge_%s as r2 where r1.to_id = r2.from_id group by r2.to_id) as t;"%(query_name, query_name)
    cur.execute(code)
    r6 = int(cur.fetchone()[0])
    TE = np.ones(8)
    TE[0b001] = r1
    TE[0b100] = r4
    TE[0b011] = r3
    TE[0b101] = r5
    TE[0b110] = r6
    con.commit()
    con.close()

def CollectTEq7():
    global TE
    con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select customer.ck, count(*) from customer_%s as customer group by customer.ck) as t;"%query_name
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r8 = int(result[0])
    else: r8 = 0
    code = "select max(count) from (select supplier.sk, count(*) from supplier_%s as supplier group by supplier.sk) as t;"%query_name
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r1 = int(result[0])
    else: r1 = 0
    r9 = r1 * r8
    code = "select max(count) from (select orders.ok, count(*) from customer_%s as customer, orders_%s as orders where customer.ck = orders.ck group by orders.ok) as t"%(query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r12 = int(result[0])
    else: r12 = 0
    code = "select max(count) from (select lineitem.ok, count(*) from lineitem_%s as lineitem, supplier_%s as supplier where lineitem.sk = supplier.sk group by lineitem.ok) as t"%(query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r3 = int(result[0])
    else: r3 = 0
    code = "select max(count) from (select orders.ck, count(*) from orders_%s as orders, lineitem_%s as lineitem, supplier_%s as supplier where orders.ok = lineitem.ok and lineitem.sk = supplier.sk group by orders.ck) as t"%(query_name, query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r7 = int(result[0])
    else: r7 = 0
    code = "select max(count) from (select lineitem.sk, count(*) from customer_%s as customer, orders_%s as orders, lineitem_%s as lineitem where customer.ck = orders.ck and orders.ok = lineitem.ok group by lineitem.sk) as t"%(query_name, query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r14 = int(result[0])
    else: r14 = 0
    r11 = r3 * r8
    r13 = r1 * r12
    TE = np.ones(16)
    TE[0b0001] = r1
    TE[0b1000] = r8
    TE[0b0011] = r3
    TE[0b1100] = r12
    TE[0b1001] = r9
    TE[0b0101] = r1
    TE[0b1010] = r8
    TE[0b1110] = r14
    TE[0b1101] = r13
    TE[0b1011] = r11
    TE[0b0111] = r7
    con.commit()
    con.close()

def CollectTEq9():
    global TE
    con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select supplier.sk, count(*) from supplier_%s as supplier group by supplier.sk) as t;"%query_name
    cur.execute(code)
    resutl = cur.fetchone()
    if resutl is not None and result[0] is not None: r8 = int(result[0])
    else: r8 = 0
    code = "select max(count) from (select orders.ok, count(*) from orders_%s as orders group by orders.ok) as t;"%query_name
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r1 = int(result[0])
    else: r1 = 0
    r9 = r1 * r8
    code = "select max(count) from (select partsupp.sk, partsupp.pk, count(*) from supplier_%s as supplier, partsupp_%s as partsupp where supplier.sk = partsupp.sk group by partsupp.sk, partsupp.pk) as t"%(query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r12 = int(result[0])
    else: r12 = 0
    code = "select max(count) from (select lineitem.sk, lineitem.pk, count(*) from lineitem_%s as lineitem, orders_%s as orders where lineitem.ok = orders.ok group by lineitem.sk, lineitem.pk) as t"%(query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r3 = int(result[0])
    else: r3 = 0
    code = "select max(count) from (select partsupp.sk, count(*) from partsupp_%s as partsupp, lineitem_%s as lineitem, orders_%s as orders where partsupp.sk = lineitem.sk and partsupp.pk = itemline.pk and lineitem.ok = orders.ok group by partsupp.sk) as t"%(query_name, query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r7 = int(result[0])
    else: r7 = 0
    code = "select max(count) from (select lineitem.ok, count(*) from supplier_%s as supplier, partsupp_%s as partsupp, lineitem_%s as lineitem where supplier.sk = partsupp.sk and partsupp.sk = lineitem.sk and partsupp.pk = lineitem.pk group by lineitem.ok) as t"%(query_name, query_name, query_name)
    cur.execute(code)
    result = cur.fetchone()
    if result is not None and result[0] is not None: r14 = int(result[0])
    else: r14 = 0
    r11 = r3 * r8
    r13 = r1 * r12
    TE = np.ones(16)
    TE[0b0001] = r1
    TE[0b1000] = r8
    TE[0b0011] = r3
    TE[0b1100] = r12
    TE[0b1001] = r9
    TE[0b0101] = r1
    TE[0b1010] = r8
    TE[0b1110] = r14
    TE[0b1101] = r13
    TE[0b1011] = r11
    TE[0b0111] = r7
    con.commit()
    con.close()

def CollectTEq9():
    return

def Fac(a, k):
    res = 1
    for i in range(k):
        res*=(a-i)
    return res

def NumberOfOnes(num):
    if num==0:
        return 0
    return num%2+NumberOfOnes(int(num/2))

def CollectTEstars(star_num):
    global TE
    star_num = star_num
    con = psycopg2.connect(database=dataset, user=psql_user, password=psql_password, host=psql_host, port=psql_port)
    cur = con.cursor()
    code = "select max(count) from (select from_id, count(*) from edge_%s group by from_id) as t;"%query_name
    cur.execute(code)
    r1 = float(cur.fetchone()[0])
    size_TE = int(math.pow(2, star_num))
    TE = np.ones(size_TE)
    for i in range(1, star_num):
        res = Fac(r1, i)
        for j in range(size_TE):
            if NumberOfOnes(j) == i:
                TE[j] = res
    con.commit()
    con.close()

def CollectTEtwo_star():
    CollectTEstars(2)

def CollectTEthree_star():
    CollectTEstars(3)

def CollectTEfour_star():
    CollectTEstars(4)

def CollectTE():
    global query_name
    if query_name == "two_path":
        CollectTEtwo_path()
    elif query_name == "three_path":
        CollectTEthree_path()
    elif query_name == "two_star":
        CollectTEtwo_star()
    elif query_name == "three_star":
        CollectTEthree_star()
    elif query_name == "four_star":
        CollectTEfour_star()
    elif query_name == "triangle":
        CollectTEtriangle()
    elif query_name == "rectangle":
        CollectTErectangle()
    elif query_name == "q7":
        CollectTEq7()
    elif query_name == "q9":
        CollectTEq9()

def BinToInt(bin_num, size):
    res = 0
    for i in range(size):
        res += pow(2, size-1-i)*bin_num[i]
    return int(res)

def IntToBin(int_num,size):
    bin_num = np.zeros(size)
    for i in range(size):
        bin_num[size-1-i] = int_num%2
        int_num = int(int_num/2)
    return bin_num

def RecCompHatTE(cur_i,relation_nums,ori_TE,cur_TE,num_k,k):
    global TE
    if cur_i==relation_nums:
        int_num = BinToInt(cur_TE,relation_nums)
        #Let 0^0=1
        if num_k==0:
            return TE[int_num]
        else:
            return TE[int_num]*math.pow(k,num_k)
    if ori_TE[cur_i]==0:
        cur_TE[cur_i]=0
        return RecCompHatTE(cur_i+1,relation_nums,ori_TE,cur_TE,num_k,k)
    else:
        cur_TE[cur_i]=1
        r1=RecCompHatTE(cur_i+1,relation_nums,ori_TE,cur_TE,num_k,k)
        cur_TE[cur_i]=0
        r2=RecCompHatTE(cur_i+1,relation_nums,ori_TE,cur_TE,num_k+1,k)
        return r1+r2
        


def ComputeRS(beta):
    global TE
    global relation_nums
    relation_nums = 0
    if query_name in ("two_star", "two_path"):
        relation_nums = 2
    elif query_name in ("three_star", "three_path", "triangle"):
        relation_nums = 3
    elif query_name in ("four_star", "rectangle", "q7", "q9"):
        relation_nums = 4
    res = 0
    max_k = int(relation_nums*1.1/beta)
    max_k = max(1,max_k)
        
    for k in range(max_k):
        #Compute TE first
        hat_TE = np.zeros(pow(2,relation_nums))
        for i in range(int(pow(2,relation_nums))):
            bin_num = IntToBin(i,relation_nums)
            new_bin_num = np.zeros(relation_nums)
            hat_TE[i] = RecCompHatTE(0,relation_nums,bin_num,new_bin_num,0,k)
        ls_i = 0
        for i in range(int(pow(2,relation_nums))-1):
            ls_i+=hat_TE[i]
        ls_i*=pow(math.e,-1*beta*k)
        if res<ls_i:
            res = ls_i
    return res

def Compute():
    global query_name
    global relation_nums
    
    relation_nums = 0
    if query_name in ("two_star", "two_path"):
        relation_nums = 2
    elif query_name in ("three_star", "three_path", "triangle"):
        relation_nums = 3
    elif query_name in ("four_star", "rectangle", 'q7', 'q9'):
        relation_nums = 4
    

    for i in range(9):
        beta = 0.05*math.pow(2,i)
        res = ComputeRS(beta)

def main(argv):
    global dataset
    dataset = ''
    global query_name
    query_name = ''
    global beta
    beta = 0
    global psql_user
    psql_user = "postgres"
    global psql_password
    psql_password = "postgres"
    global psql_host
    psql_host = None
    global psql_port
    psql_port = 5432

    try:
        opts, args = getopt.getopt(argv,"d:q:b:u:p:h:o:",["dataset=", "query_name", "beta=", "psql_user=", "psql_password=", "psql_host", "psql_port"])
    except getopt.GetoptError:
        print("ResidualSensitivity.,py -d <dataset> -q <query_name> -b <beta> -u <psql_user(default postgres)> -p <psql_password(default postgres)> -h <psql_host(defalut "")>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-d", "-dataset"):
            dataset = str(arg)
        elif opt in ("-q", "--query_name"):
            query_name = str(arg)
        elif opt in ("-b", "--beta"):
            beta = float(arg)
        elif opt in ("-u", "--psql_user"):
            psql_user = str(arg)
        elif opt in ("-p", "--psql_password"):
            psql_password = str(arg)
        elif opt in ("-h", "--psql_host"):
            psql_host = str(arg)
        elif opt in ("-o", "--psql_port"):
            psql_port = int(arg)

    if beta == 0:
        print("Invalid beta")
        sys.exit()
    if query_name not in ("two_path", "three_path", "triangle", "rectangle", "two_star", "three_star", "four_star", 'q7', 'q9'):
        print("Invalid query name")
        sys.exit()
    if dataset == '':
        print("Invalid dataset")
        sys.exit()
    
    CollectTE()
    res = ComputeRS(beta)
    return res

if __name__ == "__main__":
    res = main(sys.argv[1:])
    print(res)