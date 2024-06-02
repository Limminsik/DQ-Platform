import sys
import os
import pandas as pd

# 현재 파일의 디렉토리를 기준으로 경로 설정
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'dq_assessment_module')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'dq_management_module')))

from dq_assessment.dbconnection import connect_db, close_db
from dq_assessment.completeness import calculate_missingness
from dq_assessment.validity import check_vocab_validity

# 데이터베이스 연결 설정
dbname = "hr"
host = "localhost"
port = 5432
user = "postgres"
password = "4002"

# 데이터베이스 연결
engine = connect_db(dbname, host, port, user, password)
print("Database connection established successfully!")


# 완전성 평가 예제 실행
table_name = "hr.dept"
column_name = "deptno"

completeness_result = calculate_missingness(engine, table_name, column_name)
print("Completeness Result:", completeness_result)





# # 유효성 평가 예제 실행
# valid_values = ["Ms.", "Mr.", "Dr.", "Mrs."]
# validity_result = check_vocab_validity(engine, table_name, column_name, valid_values)
# print("Validity Result:", validity_result)





# 데이터베이스 연결 종료
close_db(engine)
print("Database connection closed.")
