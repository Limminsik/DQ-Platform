import pandas as pd
from sqlalchemy import text

nonsense = ['+', '-', '_', '#', '$', '*', '\\', '?', '.', '&', '^', '!', '@', 'NI', 'M']

def check_null_and_empty(engine, table_name, column_name):
    query = text(f"""
        SELECT COUNT(*) AS null_or_empty_count
        FROM {table_name}
        WHERE {column_name} IS NULL OR TRIM({column_name}) = '';
    """)
    result = pd.read_sql_query(query, engine)
    return result['null_or_empty_count'][0]

def check_special_chars(engine, table_name, column_name):
    special_char_conditions = " OR ".join([f"TRIM({column_name}) = '{char}'" for char in nonsense])
    query = text(f"""
        SELECT COUNT(*) AS special_char_count
        FROM {table_name}
        WHERE {special_char_conditions};
    """)
    result = pd.read_sql_query(query, engine)
    return result['special_char_count'][0]

def get_total_count(engine, table_name):
    query = text(f"""
        SELECT COUNT(*) AS total_count
        FROM {table_name};
    """)
    result = pd.read_sql_query(query, engine)
    return result['total_count'][0]

def calculate_missingness(engine, table_name, column_name):
    total_count = get_total_count(engine, table_name)
    null_and_empty_count = check_null_and_empty(engine, table_name, column_name)
    special_char_count = check_special_chars(engine, table_name, column_name)
    
    total_missing = null_and_empty_count + special_char_count
    missing_percentage = 0 if total_count == 0 else round((total_missing / total_count) * 100, 2)
    
    result = {
        'TEST_DATE': pd.Timestamp.today(),
        'TABLE_NAME': table_name,
        'COLUMN_NAME': column_name,
        'TOTAL_COUNT': total_count,
        'NULL_AND_EMPTY_COUNT': null_and_empty_count,
        'SPECIAL_CHAR_COUNT': special_char_count,
        'MISSING_PERCENTAGE': missing_percentage
    }
    
    return result
