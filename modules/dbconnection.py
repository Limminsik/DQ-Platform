import psycopg2
import pandas as pd

def connect_db(dbname, host, port, user, password):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            host=host,
            port=port,
            user=user,
            password=password
        )
        return conn
    except psycopg2.OperationalError as e:
        print("Error connecting to database:")
        print(f"DB Name: {dbname}")
        print(f"Host: {host}")
        print(f"Port: {port}")
        print(f"User: {user}")
        print(f"Password: {password}")
        print(f"Error message: {e}")
        raise

def close_db(conn):
    conn.close()

def fetch_data(conn, query):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"Error fetching data: {e}")
        raise
