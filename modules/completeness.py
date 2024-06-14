# 2. completeness
# 완전성
# - 데이터 값 완전성 : 데이터 값의 완전성 not null 컬럼에 null
# - 데이터 파일 레코드 완전성 : 데이터 파일 내 레코드 수가 완전한지 

# : nonsense 값을 확인하는 것과 결측값을 확인하는 쿼리로 제작
# + 다양한 dbms에 맞는 쿼리 추가


import pandas as pd

class Completeness:
    def __init__(self, query):
        self.query = query
        self.nonsense = ["+", "-", "_", "#", "$", "*", "\\", "?", ".", "&", "^", "%", "!", "@", "NI"]

    def postgresql_query(self, table_name, column_name):
        ms1_freq_query = f"""
            SELECT COUNT({column_name})
            FROM {table_name}
            WHERE {column_name} IS NULL OR TRIM(CAST({column_name} AS VARCHAR)) = '';
        """

        ms2_freq_query = f"""
            SELECT COUNT({column_name})
            FROM {table_name}
            WHERE TRIM(CAST({column_name} AS VARCHAR)) IN ({','.join([f"'{char}'" for char in self.nonsense])});
        """

        return ms1_freq_query, ms2_freq_query

    def calculate_completeness(self, table_name):
        completeness_results = []
        try:
            # Get column names for the table
            columns_query = f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name.split('.')[-1]}'
                AND table_schema = '{table_name.split('.')[0]}';
            """
            columns = pd.read_sql_query(columns_query, self.query.conn)['column_name'].tolist()

            for column_name in columns:
                ms1_freq_query, ms2_freq_query = self.postgresql_query(table_name, column_name)
                
                ms1_freq_result = pd.read_sql_query(ms1_freq_query, self.query.conn).iloc[0, 0]
                ms2_freq_result = pd.read_sql_query(ms2_freq_query, self.query.conn).iloc[0, 0]

                total_query = f"SELECT COUNT(*) FROM {table_name};"
                total_count = pd.read_sql_query(total_query, self.query.conn).iloc[0, 0]

                completeness_percentage = 100 - ((ms1_freq_result + ms2_freq_result) / total_count * 100)

                completeness_results.append({
                    "table": table_name,
                    "column": column_name,
                    "null_or_empty_count": ms1_freq_result,
                    "nonsense_count": ms2_freq_result,
                    "total_count": total_count,
                    "completeness_percentage": completeness_percentage
                })
        except Exception as e:
            print(f"Error calculating completeness: {e}")
            raise
        return completeness_results
