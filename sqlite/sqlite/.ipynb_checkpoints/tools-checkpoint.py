"""
Functions used to query ontime performance database.
"""

import sqlite3
import pandas as pd

"""Function used to query the performance table from the 
delay prediction database.
    
    Parameters
    ----------
    db_path : string
        Path where the performance datbase is stored.
    columns : string
        Select the columns from the table to return.
    airline : string
        Select the airline.
    dow : string
        Select the day of week.
    
    Returns
    -------
    
    Notes
    -----
    If the columns CRSDepTime and/or CRSArrTime are selected, they will be converted to the pandas date-time format before being returned.
    
    """
def query_fp(db_path,columns,airline,dow):
    conn = sqlite3.connect(db_path) #establish connection to db
    sql = "SELECT %s FROM ontime_performance WHERE UniqueCarrier= '%s' and DayOfWeek='%s';" %(columns,airline,dow)
    print(sql)
    df = pd.read_sql_query(sql,conn)
    columns_heads = list(df)
    #If the CRSDepTime of CRSArrTime columns are selected, they will be converted to Pandas datetime format
    if "CRSDepTime" in column_heads:
        df.CRSDepTime = df.CRSDepTime.astype(str).str.zfill(4)
        df.CRSDepTime = pd.to_datetime(df.CRSDepTime, format='%H%M').dt.time
    if "CRSArrTime" in column_heads:
        df.CRSArrTime = df.CRSArrTime.astype(str).str.zfill(4)
        df.CRSArrTime = pd.to_datetime(df.CRSArrTime, format='%H%M').dt.time
    return df

	
def query_hour(db_path,datatype,airline,dow,hour):
	conn = sqlite3.connect(db_path)
	hourfilter1 = hour*100
	hourfilter2 = hour*100+59


	sql = "SELECT %s FROM ontime_performance WHERE UniqueCarrier = '%s' and DayOfWeek = '%s' and %s >= '%s' and %s <= '%s';" %(datatype,airline,dow,datatype,hourfilter1,datatype,hourfilter2)
	print(sql)
	df = pd.read_sql_query(sql,conn)
	return df












