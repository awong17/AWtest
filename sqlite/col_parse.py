def col_parse(cols):
    db_cols = ""

    int_not_null = ['Quarter','Month','DayofMonth','DayOfWeek','AirlineID','OriginAirportID','OriginAirportSeqID','OriginCityMarketID','OriginWac','DestAirportID','DestAirportSeqID','DestCityMarketID','Cancelled']

    int_null = ['FlightNum','OriginStateFips','DestStateFips','DestWac','DepDelay','DepDelayMinutes','DepDel15','DepartureDelayGroups','TaxiOut','TaxiIn','ArrDelay','ArrDelayMinutes','ArrDel15','ArrivalDelayMinutes','ArrDel15','ArrivalDelayGroups','Diverted','CRSElapsedTime','ActualElapsedTime','AirTime','Flights','Distance','DistanceGroup','CarrierDelay','WeatherDelay','NASDelay','SecurityDelay','LateAircraftDelay','TotalAddGTime','LongestAddGTime','DivAirportLandings','DivReachedDest','DivActualElapsedTime','DivArrDelay','DivDistance','Div1AirportID','Div1AirportSeqID','Div1TotalGTime','Div1LongestGTime','Div2AirportID','Div2AirportSeqID','Div2TotalGTime','Div2LongestGTime','Div3AirportID','Div3AirportSeqID','Div3TotalGTime','Div3LongestGTime','Div4AirportID','Div4AirportSeqID','Div4TotalGTime','Div4LongestGTime','Div5AirportID','Div5AirportSeqID','Div5TotalGTime','Div5LongestGTime']

    date_not_null = ['FlightDate']

    time_not_null = ['CRSDepTime','CRSArrTime']

    var_10_null = ['UniqueCarrier','Carrier','Origin','OriginState','Dest','DepTime']

    var_10_not_null = ['DestState']

    var_45_null = ['TailNum','OriginCityName','OriginStateName','DestCityName','DestStateName','DepTimeBlk','WheelsOff','WheelsOn','ArrTime','ArrTimeBlk','CancellationCode','FirstDepTime','Div1Airport','Div1WheelsOn','Div1WheelsOff','Div1TailNum','Div2Airport','Div2WheelsOn','Div2WheelsOff','Div2TailNum','Div3Airport','Div3WheelsOn','Div3WheelsOff','Div3TailNum','Div4Airport','Div4WheelsOn','Div4WheelsOff','Div4TailNum','Div5Airport','Div5WheelsOn','Div5WheelsOff','Div5TailNum']

    for line in cols:
        if line == 'Year':
            db_cols = db_cols + " 'YEAR' NOT NULL"
        else if line in int_not_null:
            db_cols = db_cols + ",'" + line + "' INT NOT NULL"
        else if line in int_null:
            db_cols = db_cols + ",'" + line + "' INT NULL"
        else if line in date_not_null:
            db_cols + ",'" + line + "' DATE NOT NULL"
        else if line in time_not_null:
            db_cols + ",'" + line + "' TIME NOT NULL"
        else if line in var_10_null:
            db_cols + ",'" + line + "' VARCHAR(10) NULL"
        else if line in var_10_not_null:
            db_cols + ",'" + line + "' VARCHAR(10) NOT NULL"
        else if line in var_45_null:
            db_cols + ",'" + line + " ' VARCHAR(45) Null"
        else:
            print("Column " + line + " does not exist in data.")
    return(db_cols)