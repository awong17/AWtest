import pandas as pd
import sqlite3
from sqlalchemy import create_engine

def create_db(db_path):
    
    # Creating/connecting to the sqlite database:
    conn = sqlite3.connect(db_path) #db should be the path specified for creating db location
    
    # The path for where the database is to be created is specified followed by the actual database name ending with '.db'
    # This command not only creates the database if it does not exist yet, but it also connects to said database
    # If the database does not exist two actions will occur:
    #     1. The database will be created
    #     2. A connection to that database will be established
    # If the database already exists one action will occur:
    #     1. A connection will be established to the specified database ( no duplicate will be created)
    
    # Finally the changes are committed to the db and the connection is closed.
    conn.commit()
    conn.close()
    print('DB created succesfully at :' + str(db_path))


def create_table(db_path,cols):
    
    # Connecting to the sqlite database:
    # Specify the path and database name
    conn = sqlite3.connect(db_path)
    
    # Create a cursor object to interact with the database
    c = conn.cursor()
    
    # SQL statement that will be executed to create the ontime_performance table:
    # First line has the "CREATE TABLE IF NOT EXISTS" statement followed by the desired table name
    # This statement ensures that no duplicate tables will be created, if the table already exists no action will be taken
    
    sql = 'CREATE TABLE IF NOT EXISTS ontime_performance ('+ cols +')'
    
    # The SQL statement is then executed (this can be combined into one line as well)
    c.execute(sql)
    
    # Finally the changes are committed to the db and the connection is closed.
    # This statement is crucial otherwise the table will be created but not saved
    conn.commit()
    conn.close()
    
def insert_data(db_path,raw_path,cols):
    ### First a connection to the slqite database must be established
    conn = sqlite3.connect(db_path)
        
    ### Inserting data into the table
    
    # Reading in the csv file, ensure it is located in the same directory as the script or specify the path
    df = pd.read_csv(raw_path,low_memory=False,usecols=cols)
    
    # Preprocessing/ converting to proper time format:
    if "CRSDepTime" in cols:
        df.loc[df.CRSDepTime == 2400, 'CRSDepTime'] = 0000
        df.CRSDepTime = df.CRSDepTime.astype(str).str.zfill(4)
        df.CRSDepTime = pd.to_datetime(df.CRSDepTime, format='%H%M').dt.time
    if "CRSArrTime" in cols:
        df.loc[df.CRSArrTime == 2400, 'CRSArrTime'] = 0000
        df.CRSArrTime = df.CRSArrTime.astype(str).str.zfill(4)
        df.CRSArrTime = pd.to_datetime(df.CRSArrTime, format='%H%M').dt.time

    # Inserting the data into the table:
    engine = create_engine('sqlite:///'+db_path)
    
    pd.DataFrame.to_sql(self=df, name='ontime_performance', con=engine, if_exists='append', index=False,chunksize=100000)
    
    # Commiting the changes to the database and closing the connection (important otherwise data will not be saved):
    conn.commit()
    conn.close()

#This function is used to create the columns string which will be used to create the db, since we are taking a list of columns, but must also tell the DB what data type they should be
def col_parse(cols):
    db_cols = ""
#Each of the following variables includes all of the possible column headers from the entire performance data set
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
            db_cols = db_cols + ",'" + line + "' INT NULL"
        elif line in date_not_null:
            db_cols = db_cols + ",'" + line + "' DATE NOT NULL"
        elif line in time_not_null:
            db_cols = db_cols + ",'" + line + "' TIME NOT NULL"
        elif line in var_10_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(10) NULL"
        elif line in var_10_not_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(10) NOT NULL"
        elif line in var_45_null:
            db_cols = db_cols + ",'" + line + "' VARCHAR(45) Null"
        else:
            print("Column " + line + " does not exist in data.")
    return(db_cols)
    