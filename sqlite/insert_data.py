### Inserting the Ontime Performance Dara into the Table in the Flight Performance Database
# Written by: David Krzemien

# Import relevant libraries
import pandas as pd
import sqlite3
from sqlalchemy import create_engine

### First a connection to the slqite database must be established
conn = sqlite3.connect('/Users/allen/Documents/SQLite/data/processed/fp_db.db')

# Create a cursor object:
c = conn.cursor()


### Inserting data into the table

# Reading in the csv file, ensure it is located in the same directory as the script or specify the path
df= pd.read_csv('/Users/allen/Documents/SQLite/data/raw/On_Time_On_Time_Performance_2015_1_v1.csv',low_memory=False)

# Preprocessing/ converting to proper time format:
df.loc[df.CRSDepTime == 2400, 'CRSDepTime'] = 0000
df.CRSDepTime = df.CRSDepTime.astype(str).str.zfill(4)
df.CRSDepTime = pd.to_datetime(df.CRSDepTime, format='%H%M').dt.time

df.loc[df.CRSArrTime == 2400, 'CRSArrTime'] = 0000
df.CRSArrTime = df.CRSArrTime.astype(str).str.zfill(4)
df.CRSArrTime = pd.to_datetime(df.CRSArrTime, format='%H%M').dt.time

# Inserting the data into the table:
engine = create_engine('sqlite:////Users/allen/Documents/SQLite/data/processed/fp_db.db')
#print("dataframe created")

pd.DataFrame.to_sql(self=df, name='ontime_performance', con=engine, if_exists='append', index=False,chunksize=100000)
#pd.DataFrame.to_sql(name='ontime_performance', con=engine, if_exists='append', index=False,chunksize=100000)

# Commiting the changes to the database and closing the connection (important otherwise data will not be saved):
conn.commit()
conn.close()
