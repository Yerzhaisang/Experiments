import psycopg2
import numpy as np
import os
import pandas as pd
import os.path
import sys

sqlpath = "/home/yerzh/comp1/queries/"

print("Hello!")
onlyfiles = [f for f in os.listdir(sqlpath) if os.path.isfile(os.path.join(sqlpath, f))]

if len(sys.argv) > 1:
    onlyfiles = sys.argv[1:]

conn = None

ls=[]
lss=[]

try:

    conn = psycopg2.connect(dbname="postgres", user="postgres", password="55695387", host="127.0.0.1")

    cur = conn.cursor()

    cur.execute("SET aqo.show_details = 'on';")

    cur.execute("set aqo.mode = 'disabled';")
    conn.commit()

    for filename in onlyfiles:

        f = open(sqlpath + filename, "r")
        query = f.read()
        query = "EXPLAIN (ANALYZE ON, VERBOSE ON, FORMAT JSON) " + query
        f.close()
        print("Use file", sqlpath + filename)
        cur.execute(query)
        cur.execute(query)
        cur.execute(query)
        cur.execute(query)
        res = cur.fetchone()[0][0]

        dictt = {}

        dictt['whole'] = [res['Plan']['Plan Rows'], res['Plan']['Actual Rows']]
        dictt['nodes'] = []

        temp = res["Plan"]

        while True:
            temp = temp['Plans'][0]
            dictt['nodes'].append([temp['Plan Rows'], temp['Actual Rows']])
            if 'Plans' not in temp.keys():
                break
        L1 = 0
        L2 = 0
        for j in dictt['nodes']:
            if j[0]==0:
                aa=0
            else:
                aa=np.log(np.float32(j[0]))
            if j[1]==0:
                bb=0
            else:
                bb=np.log(np.float32(j[1]))
            L1 += np.abs(aa - bb)
            L2 += (aa - bb) * (aa - bb)
        conn.commit()
        ls.append([filename, L1, L2, res['Planning Time'], res["Execution Time"]])
        lss.append([filename, res])
    df = pd.DataFrame(ls, columns =["filename", "L1 norm of errors(log) on nodes", "L2 norm of errors(log) on nodes", "planning time", "execution time"])
    dff = pd.DataFrame(lss, columns =["filename", "plans"])
    df.to_csv("/home/yerzh/comp1/new/res.csv")
    dff.to_csv("/home/yerzh/comp1/new/plans.csv")


    cur.close()
except psycopg2.DatabaseError as error:
    print(error)
finally:
    if conn is not None:
        conn.close()

