### Creating the Flight Performance Database Ontime Performance Table Using SQLite
# Written by: David Krzemien

# Import relevant libraries (built into Python)
import sqlite3

# Connecting to the sqlite database:
# Specify the path and database name
conn = sqlite3.connect('/Users/allen/Documents/SQLite/data/processed/fp_db.db')

# Create a cursor object to interact with the database
c = conn.cursor()

# SQL statement that will be executed to create the ontime_performance table:
# First line has the "CREATE TABLE IF NOT EXISTS" statement followed by the desired table name
# This statement ensures that no duplicate tables will be created, if the table already exists no action will be taken

sql = """CREATE TABLE IF NOT EXISTS `ontime_performance` (
  `Year` YEAR NOT NULL,
  `Quarter` INT NOT NULL,
  `Month` INT NOT NULL,
  `DayofMonth` INT NOT NULL,
  `DayOfWeek` INT NOT NULL,
  `FlightDate` DATE NOT NULL,
  `UniqueCarrier` VARCHAR(10) NULL,
  `AirlineID` INT NOT NULL,
  `Carrier` VARCHAR(10) NULL,
  `TailNum` VARCHAR(45) NULL,
  `FlightNum` INT NULL,
  `OriginAirportID` INT NOT NULL,
  `OriginAirportSeqID` INT NOT NULL,
  `OriginCityMarketID` INT NOT NULL,
  `Origin` VARCHAR(10) NULL,
  `OriginCityName` VARCHAR(45) NULL,
  `OriginState` VARCHAR(10) NULL,
  `OriginStateFips` INT NULL,
  `OriginStateName` VARCHAR(45) NULL,
  `OriginWac` INT NOT NULL,
  `DestAirportID` INT NOT NULL,
  `DestAirportSeqID` INT NOT NULL,
  `DestCityMarketID` INT NOT NULL,
  `Dest` VARCHAR(10) NULL,
  `DestCityName` VARCHAR(45) NULL,
  `DestState` VARCHAR(10) NOT NULL,
  `DestStateName` VARCHAR(45) NULL,
  `DestStateFips` INT NULL,
  `DestWac` INT NULL,
  `CRSDepTime` TIME NOT NULL,
  `DepTime` VARCHAR(10) NULL,
  `DepDelay` INT NULL,
  `DepDelayMinutes` INT NULL,
  `DepDel15` INT NULL,
  `DepartureDelayGroups` INT NULL,
  `DepTimeBlk` VARCHAR(45) NULL,
  `TaxiOut` INT NULL,
  `WheelsOff` VARCHAR(45) NULL,
  `WheelsOn` VARCHAR(45) NULL,
  `TaxiIn` INT NULL,
  `CRSArrTime` TIME NOT NULL,
  `ArrTime` VARCHAR(45) NULL,
  `ArrDelay` INT NULL,
  `ArrDelayMinutes` INT NULL,
  `ArrDel15` INT NULL,
  `ArrivalDelayGroups` INT NULL,
  `ArrTimeBlk` VARCHAR(45) NULL,
  `Cancelled` INT NOT NULL,
  `CancellationCode` VARCHAR(45) NULL,
  `Diverted` INT NULL,
  `CRSElapsedTime` INT NULL,
  `ActualElapsedTime` INT NULL,
  `AirTime` INT NULL,
  `Flights` INT NULL,
  `Distance` INT NULL,
  `DistanceGroup` INT NULL,
  `CarrierDelay` INT NULL,
  `WeatherDelay` INT NULL,
  `NASDelay` INT NULL,
  `SecurityDelay` INT NULL,
  `LateAircraftDelay` INT NULL,
  `FirstDepTime` VARCHAR(45) NULL,
  `TotalAddGTime` INT NULL,
  `LongestAddGTime` INT NULL,
  `DivAirportLandings` INT NULL,
  `DivReachedDest` INT NULL,
  `DivActualElapsedTime` INT NULL,
  `DivArrDelay` INT NULL,
  `DivDistance` INT NULL,
  `Div1Airport` VARCHAR(45) NULL,
  `Div1AirportID` INT NULL,
  `Div1AirportSeqID` INT NULL,
  `Div1WheelsOn` VARCHAR(45) NULL,
  `Div1TotalGTime` INT NULL,
  `Div1LongestGTime` INT NULL,
  `Div1WheelsOff` VARCHAR(45) NULL,
  `Div1TailNum` VARCHAR(45) NULL,
  `Div2Airport` VARCHAR(45) NULL,
  `Div2AirportID` INT NULL,
  `Div2AirportSeqID` INT NULL,
  `Div2WheelsOn` VARCHAR(45) NULL,
  `Div2TotalGTime` INT NULL,
  `Div2LongestGTime` INT NULL,
  `Div2WheelsOff` VARCHAR(45) NULL,
  `Div2TailNum` VARCHAR(45) NULL,
  `Div3Airport` VARCHAR(45) NULL,
  `Div3AirportID` INT NULL,
  `Div3AirportSeqID` INT NULL,
  `Div3WheelsOn` VARCHAR(45) NULL,
  `Div3TotalGTime` INT NULL,
  `Div3LongestGTime` INT NULL,
  `Div3WheelsOff` VARCHAR(45) NULL,
  `Div3TailNum` VARCHAR(45) NULL,
  `Div4Airport` VARCHAR(45) NULL,
  `Div4AirportID` INT NULL,
  `Div4AirportSeqID` INT NULL,
  `Div4WheelsOn` VARCHAR(45) NULL,
  `Div4TotalGTime` INT NULL,
  `Div4LongestGTime` INT NULL,
  `Div4WheelsOff` VARCHAR(45) NULL,
  `Div4TailNum` VARCHAR(45) NULL,
  `Div5Airport` VARCHAR(45) NULL,
  `Div5AirportID` INT NULL,
  `Div5AirportSeqID` INT NULL,
  `Div5WheelsOn` VARCHAR(45) NULL,
  `Div5TotalGTime` INT NULL,
  `Div5LongestGTime` INT NULL,
  `Div5WheelsOff` VARCHAR(45) NULL,
  `Div5TailNum` VARCHAR(45) NULL)"""

# The SQL statement is then executed (this can be combined into one line as well)
c.execute(sql)

# Finally the changes are committed to the db and the connection is closed.
# This statement is crucial otherwise the table will be created but not saved
conn.commit()
conn.close()
