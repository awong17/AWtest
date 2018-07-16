def query_fp(db_path, sql, airline, dayofweek):
    conn = connect_db(db_path) #establish connection to db
    df = pd.read_sql_query("SELECT AirlineID,DayOfWeek FROM ontime_performance",(airline,dayofweek))
