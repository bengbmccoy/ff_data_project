# This file sets up the SQLite database and the tables to be populated.

# import required libraries
import os
import sqlite3
import sys

# point to correct directory
db_directory = "../data/sqlite/"  # Name of the directory to store the database
db_filename = "ff_proj.db"

# check direcotry exists and try to create if not exists
if not os.path.exists(db_directory):
    print("Directory does not exist, creating..")
    
    try:
        os.makedirs(db_directory)
        print("Directory successfully created")
    
    except:
        print("Could not create directory")
        sys.exit(1)

# initialise the database and try to connect to it
db_path = os.path.join(db_directory, db_filename)
try:
    conn = sqlite3.connect(db_path)
except:
    print("Could not connect to database")
    sys.exit(1)

conn.close()