"""
Functions used to create the database for the raw ontime performance data.
"""

import pandas as pd
import sqlite3
from sqlalchemy import create_engine


def create_db(db_path):
    """Creates the database for the performance ontime performance data. 
    At the specified db path a database will either create the database file 
    if it does not exist and then connect to it, 
    or it will connect to the file if it already exists. After connecting to the
    db, it will close the db file.
    
    Parameters
    ----------
    db_path : string
        Path where the performance datbase is stored.
    
    Returns
    -------
    
    Notes
    -----
    Function returns nothing but will create the db file at the specified path.
    
    """
    conn = sqlite3.connect(db_path) #db should be the path specified for creating db location
    conn.commit()
    conn.close()


def create_table(db_path,db_cols):
    """Creates the columns in the ontime performance database. 
    At the specified db path a table name 'ontime_performance' will be created if 
    one does not exist. It will also create the db at the specified path if it does not 
    exist yet.
   
    Parameters
    ----------
    db_path : string
        Path where the performance datbase is stored.
    db_cols : string
        Specify the column name and data type to create in SQL.
    
    Returns
    -------
    
    Notes
    -----
    Since SQL command to take the columns is a string that requires data types and the pandas command
    (used in insert_data function) to import the db data is a list, the cols list needs to be converted
    to the SQL command which is handled by the col_parse function
    
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    sql = 'CREATE TABLE IF NOT EXISTS ontime_performance (%s)' %(db_cols,)
    
    c.execute(sql)
    
    conn.commit()
    conn.close()
    
    
def insert_data(db_path,raw_folder,raw_path,cols):
    """Creates the columns in the ontime performance database. 
    At the specified db path a table name 'ontime_performance' will be created if 
    one does not exist.
   
    Parameters
    ----------
    db_path : string
        Path where the performance datbase is stored.
    raw_path : string
        Path to the raw folder. Used in add_timezone function.
    raw_path : string
        Path where the raw performance data file is stored.
    cols : list
        Specify the column names to import from the raw data.
    
    Returns
    -------
    
    Notes
    -----
    Since SQL command to take the columns is a string that requires data types and the pandas command
    (used in insert_data function) to import the db data is a list, the cols list needs to be converted
    to the SQL command which is handled by the col_parse function
    
    """
    
    conn = sqlite3.connect(db_path)
        
    df = pd.read_csv(raw_path,low_memory=False,usecols=cols)
    
    # Preprocessing/ converting to proper time format:
    if "CRSDepTime" in cols:
        df.loc[df.CRSDepTime == 2400, 'CRSDepTime'] = 0000
        df.CRSDepTime = df.CRSDepTime.astype(str).str.zfill(4)
    if "CRSArrTime" in cols:
        df.loc[df.CRSArrTime == 2400, 'CRSArrTime'] = 0000
        df.CRSArrTime = df.CRSArrTime.astype(str).str.zfill(4)
    if 'Origin' in cols:
        airports = df.['Origin'].tolist()
        timezone_list = add_timezone(raw_folder,airports)
        df.append(pd.DataFrame(timezone_list), columns=['Timezone'])

    # Inserting the data into the table:
    engine = create_engine('sqlite:///'+db_path)
    
    pd.DataFrame.to_sql(self=df, name='ontime_performance', con=engine, if_exists='append', index=False,chunksize=100000)
    
    conn.commit()
    conn.close()
    
def add_timezone(raw_folder,airports):
    timezone_file = '%s/airport_timezone.csv'%(raw_folder,)
    timezones = []
    tz = pd.read_csv(timezone_file,usecols=['IATA','Timezone'])
    #Take the list of airports and create a list of timezones based on the the timezone csv.
    for airport in airports:
        timezones.append(tz.loc[tz['IATA']==airport,'Timezone'].iloc[0])
    return(timezones)
    

def col_parse(cols):
    """Creates the columns string which will be used to create the db, 
    since we are taking a list of columns, the list must be converted
    to a string including the data type to be used as a SQL command.
    The function has all possible column names and will give them the 
    approriate type. If name is given that is not a column in the data, 
    it will return "Columns does not exist in data".
   
    Parameters
    ----------
    cols : list
        Specify the column names to import from the raw data.
        
    Returns
    -------
    db_cols : string
        The columns with type in the db.
    
    Notes
    -----
    Since SQL command to take the columns is a string that requires data types and the pandas command
    (used in insert_data function) to import the db data is a list, the cols list needs to be converted
    to the SQL command which is handled by the col_parse function
    
    """
    
    db_cols = ""
    
    int_not_null = ['Quarter','Month','DayofMonth','DayOfWeek','AirlineID','OriginAirportID','OriginAirportSeqID','OriginCityMarketID','OriginWac','DestAirportID','DestAirportSeqID','DestCityMarketID','Cancelled']

    int_null = ['FlightNum','OriginStateFips','DestStateFips','DestWac','DepDelay','DepDelayMinutes','DepDel15','DepartureDelayGroups','TaxiOut','TaxiIn','ArrDelay','ArrDelayMinutes','ArrDel15','ArrivalDelayMinutes','ArrDel15','ArrivalDelayGroups','Diverted','CRSElapsedTime','ActualElapsedTime','AirTime','Flights','Distance','DistanceGroup','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay','TotalAddGTime','LongestAddGTime','DivAirportLandings','DivReachedDest','DivActualElapsedTime','DivArrDelay','DivDistance','Div1AirportID','Div1AirportSeqID','Div1TotalGTime','Div1LongestGTime','Div2AirportID','Div2AirportSeqID','Div2TotalGTime','Div2LongestGTime','Div3AirportID','Div3AirportSeqID','Div3TotalGTime','Div3LongestGTime','Div4AirportID','Div4AirportSeqID','Div4TotalGTime','Div4LongestGTime','Div5AirportID','Div5AirportSeqID','Div5TotalGTime','Div5LongestGTime']

    date_not_null = ['FlightDate']

    time_not_null = ['CRSDepTime','CRSArrTime']

    var_10_null = ['UniqueCarrier','Carrier','Origin','OriginState','Dest','DepTime']

    var_10_not_null = ['DestState']

    var_45_null = ['TailNum','OriginCityName','OriginStateName','DestCityName','DestStateName','DepTimeBlk','WheelsOff','WheelsOn','ArrTime','ArrTimeBlk','CancellationCode','FirstDepTime','Div1Airport','Div1WheelsOn','Div1WheelsOff','Div1TailNum','Div2Airport','Div2WheelsOn','Div2WheelsOff','Div2TailNum','Div3Airport','Div3WheelsOn','Div3WheelsOff','Div3TailNum','Div4Airport','Div4WheelsOn','Div4WheelsOff','Div4TailNum','Div5Airport','Div5WheelsOn','Div5WheelsOff','Div5TailNum']

    #Read the provided colns list and create the string for the db columns while appending the data types. If a column name is given but it not a column given in the raw data, it will print does not exist.
    for line in cols:
        if line == 'Year':
            db_cols = db_cols + " 'YEAR' NOT NULL"
        elif line in int_not_null:
            db_cols = db_cols + ",'" + line + "' INT NOT NULL"
        elif line in int_null:
            db_cols = db_cols + ",'" + line + "' INT"
        elif line in date_not_null:
            db_cols = db_cols + ",'" + line + "' DATE NOT NULL"
        elif line in time_not_null:
            db_cols = db_cols + ",'" + line + "' TIME NOT NULL"
        elif line in var_10_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(10) NULL"
        elif line in var_10_not_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(10) NOT NULL"
        elif line in var_45_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(45)"
        else:
            print("Column %s does not exist in data." %line)
    return(db_cols)
    