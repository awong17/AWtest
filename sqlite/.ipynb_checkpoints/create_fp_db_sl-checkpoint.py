### Creating the Flight Performance Database Using sqlite
# Written by: David Krzemien

# Import relevant libraries (built into Python)
import sqlite3

# Creating/connecting to the sqlite database:
conn = sqlite3.connect('/Users/allen/Documents/SQLite/data/processed/fp_db.db')

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