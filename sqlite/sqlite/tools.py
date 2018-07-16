import sqlite3
import pandas as pd

def query_fp(db_path,airline,dow):
    conn = sqlite3.connect(db_path) #establish connection to db
    #cur = conn.cursor()
    sql = "SELECT * FROM ontime_performance WHERE UniqueCarrier='%s' and DayOfWeek='%s';" %(airline,dow)
    print(sql)
    df = pd.read_sql_query(sql,conn)
    return df
