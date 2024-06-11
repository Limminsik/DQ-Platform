import sys
import os
import pandas as pd
from psycopg2 import OperationalError
import traceback

# Add modules to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'modules')))

from modules.query import Query
from modules.completeness import completeness

# Database connection settings from config file
config_file = "config.json"

try:
    # Create a Query instance
    query_instance = Query(config_file)
    print("Database connection established successfully!")

    # Example query to fetch data
    table_name = "hr.emp"
    query = f"SELECT * FROM {table_name} LIMIT 10;"
    
    # Fetch data into a DataFrame
    data = query_instance.fetch_data(query)
    print("Data loaded successfully:")
    print(data)
    
    # Close the database connection
    query_instance.close_db()
    print("Database connection closed.")

except OperationalError as e:
    print(f"An error occurred: {e}")
    traceback.print_exc()
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    traceback.print_exc()


completeness(query).completeness
print("Completeness Test has been run")