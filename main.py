import sys
import os
import pandas as pd
from psycopg2 import OperationalError
import traceback

# Add dq_assessment_module to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'dq_assessment')))

from dbconnection import connect_db, close_db, fetch_data

# Database connection settings
dbname = "hr"
host = "localhost"
port = 5432
user = "postgres"
password = "4002"

try:
    # Connect to the database
    conn = connect_db(dbname, host, port, user, password)
    print("Database connection established successfully!")
    
    # Example query to fetch data
    table_name = "dept"
    query = f"SELECT * FROM {table_name} LIMIT 10;"
    
    # Fetch data into a DataFrame
    data = fetch_data(conn, query)
    print("Data loaded successfully:")
    print(data)
    
    # Close the database connection
    close_db(conn)
    print("Database connection closed.")

except OperationalError as e:
    print(f"An error occurred: {e}")
    traceback.print_exc()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    traceback.print_exc()
