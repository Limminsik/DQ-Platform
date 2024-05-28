# DB Connection

pip install sqlalchemy

# PostgreSQL 연결 설정
import pandas as pd
import sqlalchemy

from sqlalchemy import create_engine

def connect_db(dbname, host, port, user, password):
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    return engine

def close_db(engine):
    engine.dispose()
