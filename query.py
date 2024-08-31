import duckdb
import pandas as pd
from datetime import datetime

def query_sandbox(db_name, query_statement):
    conn = duckdb.connect(db_name)
    df = conn.execute(query_statement).fetchdf()
    conn.close()
    print(df)

# Usage
db_name = 'budgeting-tool.db'
table_name = 'consolidated_transactions'
sandbox_query = f"""
    select * from consolidated_transactions where Category = 'Bills & Utilities'
"""
query_sandbox(db_name, sandbox_query)
