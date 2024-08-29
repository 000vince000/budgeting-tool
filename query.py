import duckdb
import pandas as pd
from datetime import datetime


def query_latest_month(db_name, table_name):
    conn = duckdb.connect(db_name)
	
    query = f"""
        WITH latest_month AS (
            SELECT MAX(DATE_TRUNC('month', "Transaction Date")) as max_month
            FROM {table_name}
        ),
        
        latest_month_sums AS (
            SELECT 
                Category,
                SUM(Amount)*-1 as latest_sum
            FROM {table_name}
            WHERE DATE_TRUNC('month', "Transaction Date") = (SELECT max_month FROM latest_month)
            GROUP BY Category
        ),
        
        historic_percentiles AS (
            SELECT 
                Category,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monthly_sum) as p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY monthly_sum) as p75
            FROM (
                SELECT 
                    Category,
                    DATE_TRUNC('month', "Transaction Date") as month,
                    SUM(Amount)*-1 as monthly_sum
                FROM {table_name}
                GROUP BY Category, DATE_TRUNC('month', "Transaction Date")
            ) monthly_sums
            GROUP BY Category
        )
        
        SELECT 
            lms.Category,
            lms.latest_sum,
            hp.p50 as historic_p50,
            hp.p75 as historic_p75
        FROM latest_month_sums lms
        JOIN historic_percentiles hp ON lms.Category = hp.Category
        ORDER BY lms.latest_sum DESC
    """
    df = conn.execute(query).fetchdf()
    conn.close()
    print(df)
    print(df.describe())
    # Or to see the categories with the highest latest sum:
    print(df.nlargest(5, 'latest_sum'))

def query_sandbox(db_name, query_statement):
    conn = duckdb.connect(db_name)
    df = conn.execute(query_statement).fetchdf()
    conn.close()
    print(df)

# Usage
db_name = 'budgeting-tool.db'
table_name = 'consolidated_transactions'
query_latest_month(db_name, table_name)


sandbox_query = f"""
    select * from consolidated_transactions where Category = 'Bills & Utilities'
"""
query_sandbox(db_name, sandbox_query)
