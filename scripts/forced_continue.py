import psycopg2
import numpy as np
import os
import pandas as pd
import os.path
import sys

sqlpath = "/home/yerzh/comp1/queries/"

print("Hello!")
onlyfiles = [f for f in os.listdir(sqlpath) if os.path.isfile(os.path.join(sqlpath, f))]

n_epochs=12

if len(sys.argv) > 1:
    onlyfiles = sys.argv[1:]

conn = None

df = pd.read_csv("/home/yerzh/comp1/new/forced_res.csv", index_col=0)
dff = pd.read_csv("/home/yerzh/comp1/new/forced_plans.csv", index_col=0)

try:

    conn = psycopg2.connect(dbname="postgres", user="postgres", password="55695387", host="127.0.0.1")

    cur = conn.cursor()

    cur.execute("SET aqo.show_details = 'on';")

    cur.execute("set aqo.mode = 'forced';")

    for epoch in range(n_epochs):

        for filename in onlyfiles:

            f = open(sqlpath + filename, "r")
            query = f.read()
            query = "EXPLAIN (ANALYZE ON, VERBOSE ON, FORMAT JSON) " + query
            f.close()
            print("Use file", sqlpath + filename, "epoch", epoch+1, "predicting")
            if not ((df['filename']==filename) & (df['#epoch']==epoch+1) & (df['#iter']=='predicting')).any():
                cur.execute("set aqo.forced_learning = false;")
                conn.commit()

                cur.execute("SET statement_timeout = '1800s'")
                conn.commit()
                try:
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

                    df.loc[len(df)] = [filename, epoch+1, "predicting", L1, L2, res['Planning Time'], res["Execution Time"], res["JOINS"]]
                    dff.loc[len(dff)] = [filename, epoch+1, "predicting", res]
                    conn.commit()
                except psycopg2.extensions.QueryCanceledError:
                    df.loc[len(df)] = [filename, epoch+1, "predicting", "inf", "inf", "inf", "inf", "inf"]
                    dff.loc[len(dff)] = [filename, epoch+1, "predicting", "inf"]
                    cur.execute("ROLLBACK")
                    conn.commit()

            if not ((df['filename']==filename) & (df['#epoch']==epoch+1) & (df['#iter']=='learning')).any():
                cur.execute("set aqo.forced_learning = true;")
                conn.commit()
                print("Use file", sqlpath + filename, "epoch", epoch+1, "learning")

                cur.execute("SET statement_timeout = '1800s'")
                conn.commit()

                try:
                    cur.execute(query)
                    res = cur.fetchone()[0][0]
                    assert res["AQO mode"] == "FORCED"
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

                    df.loc[len(df)] = [filename, epoch+1, "learning", L1, L2, res['Planning Time'], res["Execution Time"], res["JOINS"]]
                    dff.loc[len(dff)] = [filename, epoch+1, "learning", res]
                    conn.commit()
                except psycopg2.extensions.QueryCanceledError:
                    df.loc[len(df)] = [filename, epoch+1, "learning", "inf", "inf", "inf", "inf", "inf"]
                    dff.loc[len(dff)] = [filename, epoch+1, "learning", "inf"]
                    cur.execute("ROLLBACK")
                    conn.commit()

            df.to_csv("/home/yerzh/comp1/new/forced_res.csv")
            dff.to_csv("/home/yerzh/comp1/new/forced_plans.csv")


    cur.close()
except psycopg2.DatabaseError as error:
    print(error)
finally:
    if conn is not None:
        conn.close()
