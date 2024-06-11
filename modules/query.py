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

        if self.DBMS == "oracle":
                ## cx_oracle can be difficult/finicky to download for new python users.
                #  if oracle is not being used, we'll just skip this whole thing

                self.conn = self.Oracle()
        elif self.DBMS == "postgresql":
                self.conn = self.PostgreSQL()
        elif self.DBMS == "redshift":
                self.conn = self.Redshift()
        elif self.DBMS == "sql server":
                self.conn = self.SQLServer()
        elif self.DBMS == "":
                raise NameError("No DBMS defined in config.json")
        else:
                raise NameError("'%s' is not an accepted DBMS" % self.DBMS)
            ## --------------------------------------------------


    def Oracle(self):
        try:
            import cx_Oracle as Oracle
        except ModuleNotFoundError:
            import cx_oracle as Oracle
        conn = Oracle.connect(self.user + "/" + self.password + "@" + self.database)

        return conn


    def PostgreSQL(self):
        import psycopg2 as postgresql
        conn = postgresql.connect(database=self.database,
                                  user=self.user,
                                  password=self.password,
                                  host=self.host,
                                  port=self.port)
        return conn


    def Redshift(self):
        import psycopg2 as postgresql
        conn = postgresql.connect(database=self.database,
                                  user=self.user,
                                  password=self.password,
                                  host=self.host,
                                  port=self.port)
        return conn


    def SQLServer(self):
        import pyodbc as sqlserver
        driver: str = self.config["ConnectionDetails"]["Driver"]
        conn = sqlserver.connect("DRIVER=" + driver +
                                 ";SERVER=" + self.host +
                                 ";DATABASE=" + self.database +
                                 ";UID=" + self.user +
                                 ";PWD=" + self.password)
        return conn

    def close_db(self):
        self.conn.close()

    def fetch_data(self, query: str):
        try:
            return pd.read_sql_query(query, self.conn)
        except Exception as e:
            print(f"Error fetching data: {e}")
            raise