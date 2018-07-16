import pandas as pd
import sqlite3
from sqlalchemy import create_engine

def create_table(db_path,cols):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    
    sql = 'CREATE TABLE IF NOT EXISTS metar_data (%s)' %cols
    c.execute (sql)
    
    conn.commit()
    conn.close()
    
def insert_data(raw_path,cols):
    conn = sqlite3.connect(db_path)
    
    pd.read_csv = pd.read_csv(raw_path,usercols=cols)
    
    engine = create_engine('sqlite:///'+db_path)
    pd.DataFrame.to_sql(self=df, name='metar', con=engine, if_exists='append', index=False,chunksize=100000)
    
    conn.commit()
    conn.close()