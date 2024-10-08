import duckdb
import pandas as pd
from datetime import datetime
import db_operations
import create_schema

def check_primary_key(conn, table_name):
    quoted_table_name = f"'{table_name}'"
    check_pk_query = f"""
    SELECT COUNT(*)
    FROM information_schema.table_constraints
    WHERE table_name = {quoted_table_name} AND constraint_type = 'PRIMARY KEY';
    """
    pk_count = conn.execute(check_pk_query).fetchone()[0]
    if pk_count != 1:
        raise ValueError(f'Fatal error: primary key missing from table {table_name}')

def insert_data(conn, table_name, data, column_names):
    quoted_table_name = f'"{table_name}"'
    inserted_count = 0
    error_count = 0

    for item in data:
        placeholders = ', '.join(['?' for _ in column_names])
        insert_query = f"INSERT INTO {quoted_table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
        try:
            db_operations.execute_query(conn, insert_query, item if isinstance(item, tuple) else (item,))
            inserted_count += 1
        except duckdb.ConstraintException as ce:
            print(f"ConstraintException for row {item}")
            error_count += 1
        except Exception as e:
            print(f"Error inserting row {item}")
            print(f"stack trace: {e}")
            error_count += 1

    print(f"Insertion complete. Rows inserted: {inserted_count}, Rows failed: {error_count}")

def verify_data(conn, table_name):
    quoted_table_name = f'"{table_name}"'
    row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_table_name}").fetchone()[0]
    print(f"Total rows in table: {row_count}")

    sample_data = conn.execute(f"SELECT * FROM {quoted_table_name} LIMIT 5").fetchall()
    print("Sample data:")
    for row in sample_data:
        print(row)

def populate_table(db_name, table_name, data, columns):
    conn = duckdb.connect(db_name)
    try:
        create_schema.create_table(conn, table_name, columns)
        check_primary_key(conn, table_name)
        column_names = [col.split()[0] for col in columns]  # Extract column names without types
        insert_data(conn, table_name, data, column_names)
        verify_data(conn, table_name)
        conn.commit()
        print("\nAll operations completed successfully")
    except Exception as e:
        print(f"\nAn error occurred during the process: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

# Usage
db_name = 'budgeting-tool.db'
table_name = 'category_matching_patterns'
category_map = {
    "AMZN": "Amazon",
    "AMAZON": "Amazon",
    "COFFEE": "Drink",
    "CAFE": "Drink",
    "BLACK FOREST": "Drink",
    "BAKERY": "Drink",
    "CIAO GLORIA": "Drink",
    "CIAO  GLORIA": "Drink",
    "BITTERSWEET": "Drink",
    "Netflix": "Entertainment",
    "Prime Video": "Entertainment",
    "Spotify": "Entertainment",
    "BLUE CROSS": "Health & Wellness",
    'Deposit Mobile Banking': 'Health & Wellness',
    "NEW YORK MAGAZINE": "Kat spending",
    "NYTimes": "Kat spending",
    "nuuly.com": "Kat spending",
    "BURNABY PRCS BON": "Kids",
    'NAJERA-ESTEBAN': 'Kids',
    'NYCSHININGSMILES NYCSHINING': 'Kids',
    "AIRALO": "Monthly fixed cost",
    "ALOHI": "Monthly fixed cost",
    "CONDO INS": "Monthly fixed cost",
    "CUBESMART": "Monthly fixed cost",
    "GEICO": "Monthly fixed cost",
    "GOOGLE *FI": "Monthly fixed cost",
    "MBRSHIP - INTERNAL": "Monthly fixed cost",
    "ROGERS": "Monthly fixed cost",
    "USAA INSURANCE PAYMENT": "Monthly fixed cost",
    'MORTGAGE': 'Monthly mortgage expense',
    'WEB PMTS': 'Monthly property expense',
    'JESSE D VANDENBERGH': 'Rental income',
    'GRUBHUB HOLDING': 'Salary',
    'CHARLIE CHEN': 'Transportation',
    "CITIBIK": "Transportation",
    'E-ZPASS': 'Transportation',
    "LYFT": "Transportation",
    'MTA*NYCT PAYGO': "Transportation",
    'NYC FINANCE PARKING': 'Transportation',
    "UBER": "Transportation",
    "CLAUDE.AI": "Vince spending",
    "Google One": "Vince spending",
    "Google Storage": "Vince spending",
    "Patreon": "Vince spending"
}
populate_table(db_name, table_name, category_map.items(), ['keyword VARCHAR PRIMARY KEY', 'category VARCHAR'])

global_category_list = {
    'Amazon':"Discretionary",
    'Amusement':"Discretionary",
    'Automotive':"Non-discretionary",
    'Drink':"Discretionary",
    'Education':"Non-discretionary",
    'Entertainment':"Discretionary",
    'Fees & Adjustments':"Non-discretionary",
    'Food & Drink':"Discretionary",
    'Gas':"Discretionary",
    'Gifts & Donations':"Discretionary",
    'Groceries':"Non-discretionary",
    'Health & Wellness':"Non-discretionary",
    'Home':"Non-discretionary",
    'Kat spending':"Discretionary",
    'Kids':"Non-discretionary",
    'Misc':"Misc",
    'Monthly fixed cost':"Non-discretionary",
    'Monthly mortgage expense':"Cost of revenue",
    'Monthly property expense':"Cost of revenue",
    'Rental income':"Revenue",
    'Salary':"Revenue",
    'Shopping':"Discretionary",
    'Transportation':"Non-discretionary",
    'Travel':"Discretionary",
    'Vince spending':"Discretionary"
}

table_name = 'categories'
populate_table(db_name, table_name, global_category_list, ['category VARCHAR PRIMARY KEY','category_group VARCHAR'])
