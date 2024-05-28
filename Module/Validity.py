import pandas as pd
from sqlalchemy import text

def check_vocab_validity(engine, table_name, column_name, valid_values):
    query = text(f"""
        SELECT {column_name} AS value
        FROM {table_name};
    """)
    data = pd.read_sql_query(query, engine)
    data['valid'] = data['value'].isin(valid_values)
    invalid_count = data['valid'].value_counts().get(False, 0)
    invalid_percentage = round((invalid_count / len(data)) * 100, 2)
    
    return invalid_percentage
