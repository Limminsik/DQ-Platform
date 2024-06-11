import datetime
import pandas as pd

class completeness:
    def __init__(self, query: object):
        self.query = query
        self.nonsense = ["+", "-", "_", "#", "$", "*", "\\", "?", ".", "&", "^", "%", "!", "@", "NI"]



    def postgresql_query(self, table_name, column_name):
        ms1_freq_query = f"""
            SELECT COUNT({column_name})
            FROM {table_name}
            WHERE {column_name} IS NULL OR CAST({column_name} AS VARCHAR) IN ('');
        """

        ms2_freq_query = f"""
            SELECT COUNT({column_name})
            FROM {table_name}
            WHERE CAST({column_name} AS VARCHAR) IN ({','.join([f"'{char}'" for char in self.nonsense])});
        """

        return ms1_freq_query, ms2_freq_query
    

    def calculate_completeness(self, table_name, column_name):
        try:
            ms1_freq_query, ms2_freq_query = self.postgresql_query(table_name, column_name)
            
            ms1_freq_result = pd.read_sql_query(ms1_freq_query, self.query.conn).iloc[0, 0]
            ms2_freq_result = pd.read_sql_query(ms2_freq_query, self.query.conn).iloc[0, 0]

            total_query = f"SELECT COUNT(*) FROM {table_name};"
            total_count = pd.read_sql_query(total_query, self.query.conn).iloc[0, 0]

            completeness_percentage = 100 - ((ms1_freq_result + ms2_freq_result) / total_count * 100)

            return {
                "table": table_name,
                "column": column_name,
                "null_or_empty_count": ms1_freq_result,
                "nonsense_count": ms2_freq_result,
                "total_count": total_count,
                "completeness_percentage": completeness_percentage
            }
        except Exception as e:
            print(f"Error calculating completeness: {e}")
            raise