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
    
    pd.read_csv = pd.read_csv(raw_path,usecols=cols)
    
    db_cols = col_parse(cols)
    
    engine = create_engine('sqlite:///%s'% (db_path,))
    pd.DataFrame.to_sql(self=df, name='metar', con=engine, if_exists='append', index=False,chunksize=100000)
    
    conn.commit()
    conn.close()
    
def col_parse(cols):    
    db_cols = ""
    
    char_3_not_null = ['station']
    
    char_16_not_null = ['valid']
    
    varchar_3 = ['skyc1','skyc2','skyc3','skyc4']
    
    int_not_null = ['lon','lat']
    
    int_null = ['drct','sknt','gust','skyl1','skyl2','skyl3','skyl4']
    
    float_3 = ['vsby']
    
    float_4 = ['alti']
    
    float_5 = ['tmpf','dwpf','relh','p01i',]
  
    float_7 = ['mslp']
    
    for line in cols:
        if line in char_3_not_null:
            db_cols = db_cols + " 'CHAR(3) NOT NULL'"
        elif line in char_16_not_null:
            db_cols = db_cols + ",'" + line + "' CHAR(6) NOT NULL'"
        elif line in varchar_3:
            db_cols = db_cols + ",'" + line + "' VARCHAR(3)"
        elif line in int_not_null:
            db_cols = db_cols + ",'" + line + "' INT NOT NULL"
        elif line in int_null:
            db_cols = db_cols + ",'" + line + "' INT"
        elif line in float_3:
            db_cols = db_cols + ",'" + line + "' FLOAT(3)"
        elif line in float_4:
            db_cols = db_cols + ",'" + line + "' FLOAT(4)"
        elif line in float_5:
            db_cols = db_cols + ",'" + line + "' FLOAT(5)"
        elif line in float_7:
            db_cols = db_cols + ",'" + line + "' FLOAT(7)"
        else:
            print("Column %s does not exist in data." %line)
    return(db_cols)