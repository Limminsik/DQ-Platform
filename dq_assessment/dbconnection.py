# DB Connection

import pandas as pd
from sqlalchemy import create_engine

# def connect_db(dbname, host, port, user, password):
#     engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
#     return engine

def connect_db(dbname, host, port, user, password):
    engine = create_engine(f'postgresql+psycopg2://{postgres}:{4002}@{localhost}:{5432}/{dqproject}')
    return engine

def close_db(engine):
    engine.dispose()



