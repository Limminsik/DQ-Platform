import json
import os
import pandas as pd
import psycopg2
from typing import Dict, List

class Query:
    def __init__(self, config_file: str = "C:/Users/ms400/OneDrive/바탕 화면/DQproject/DQproject/config.json", vis: str ='site'):
        self._config_file = config_file
        self.vis = vis
        
        with open(config_file) as json_file:
         self.config = json.load(json_file)

        self.DBMS: str = self.config["DBMS"].lower()
        self.database: str = self.config["database"]
        self.schema: str = self.config["schema"]
        self.vocab_schema: str = self.config["vocabulary schema"]

        self.user: str = self.config['Credentials']['User']
        self.password: str = self.config['Credentials']['Password']
        
        self.host: str = self.config["ConnectionDetails"]["Host"]
        self.port: str = self.config["ConnectionDetails"]["Port"]

        self.conn = self.connect_db()

    def connect_db(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            return conn
        except psycopg2.OperationalError as e:
            print("Error connecting to database:")
            print(f"Host: {self.host}")
            print(f"Port: {self.port}")
            print(f"User: {self.user}")
            print(f"Password: {self.password}")
            print(f"Error message: {e}")
            raise

    def close_db(self):
        self.conn.close()

    def fetch_data(self, query: str):
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise