# DB Connection

import psycopg2
from sqlalchemy import create_engine

def connect_db(dbname, host, port, user, password):
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
    return engine

def close_db(engine):
    engine.dispose()