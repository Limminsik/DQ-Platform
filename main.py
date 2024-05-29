from Module.dbconnection import connect_db, close_db
from Module.completeness import calculate_missingness
from Module.validity import check_vocab_validity
import pandas as pd

# 데이터베이스 연결 설정
dbname = "your_database_name"
host = "your_host"
port = 5432
user = "your_username"
password = "your_password"

engine = connect_db(dbname, host, port, user, password)

# 완전성 평가 예제 실행
table_name = "your_schema.your_table"
column_name = "title_of_courtesy"

completeness_result = calculate_missingness(engine, table_name, column_name)
print("Completeness Result:", completeness_result)

# 유효성 평가 예제 실행
valid_values = ["Ms.", "Mr.", "Dr.", "Mrs."]
validity_result = check_vocab_validity(engine, table_name, column_name, valid_values)
print("Validity Result:", validity_result)

# 데이터베이스 연결 종료
close_db(engine)
