import duckdb
import pandas as pd
from datetime import datetime

def create_and_insert(db_name, table_name, dictionary):
    conn = duckdb.connect(db_name)
    
    # Quote the table name to handle special characters and numbers
    quoted_table_name = f'"{table_name}"'
    
    try:
        # Step 0: CAUTION!! THIS IS TO BE REMOVED TODO
        print("\nStep 0: CAUTION!! THIS IS TO BE REMOVED")
        create_table_query = f"""
        DROP TABLE IF EXISTS {quoted_table_name}
        """
        print(f"Drop table query:\n{create_table_query}")
        conn.execute(create_table_query)
        
        # Step 1: Create table
        print("\nStep 1: Creating table")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table_name} (
            keyword VARCHAR PRIMARY KEY,
            category VARCHAR 
        )
        """
        print(f"Create table query:\n{create_table_query}")
        conn.execute(create_table_query)
        
        # Step 3: Check if a primary key exists on the table
        check_pk = f"""
        SELECT COUNT(*)
        FROM information_schema.table_constraints
        WHERE table_name = {quoted_table_name.replace('"', "'")} AND constraint_type = 'PRIMARY KEY';
        """
        pk_count = conn.execute(check_pk).fetchone()[0]

        if pk_count != 1:
            print('Fatal error: primary key missing from table {quoted_table_name}')
            return

        # Step 4: Insert data
        print("\nStep 4: Inserting data")
        inserted_count = 0
        error_count = 0

        for k,v in dictionary.items():
            insert_query = f"""
            INSERT INTO {quoted_table_name} (keyword, category) VALUES ('{k}', '{v}')
            """
            try:
                conn.execute(insert_query)
                inserted_count += 1
            except duckdb.ConstraintException as ce:
                print(f"ConstraintException for row {k}: {v}")
                error_count += 1
            except Exception as e:
                print(f"Error inserting row {k}: {v}")
                print(f"stack trace: {e}")
                error_count += 1

        print(f"Insertion complete. Rows inserted: {inserted_count}, Rows failed: {error_count}")

        # Step 5: Verify data
        print("\nStep 5: Verifying data")
        row_count = conn.execute(f"SELECT COUNT(*) FROM {quoted_table_name}").fetchone()[0]
        print(f"Total rows in table: {row_count}")
        
        sample_data = conn.execute(f"SELECT * FROM {quoted_table_name} LIMIT 5").fetchall()
        print("Sample data:")
        for row in sample_data:
            print(row)

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
    "Netflix": "Entertainment",
    "CUBESMART": "Monthly fixed cost",
    "Patreon": "Vince spending",
    "NYTimes": "Kat spending",
    "AIRALO": "Monthly fixed cost",
    "Spotify USA": "Monthly fixed cost",
    "NEW YORK MAGAZINE": "Kat spending",
    "USAA INSURANCE PAYMENT": "Monthly fixed cost",
    "LYFT": "Transportation",
    "AMZN Mktp": "Shopping",
    "COFFEE": "Drink",
    "CAFE": "Drink",
    "nuuly.com": "Kat spending",
    "Prime Video Channels": "Entertainment",
    "Google Storage": "Vince spending",
    "Google One": "Vince spending",
    "UBER": "Transportation",
    "MBRSHIP - INTERNAL": "Monthly fixed cost",
    "BURNABY PRCS BON": "Kids",
    "BLACK FOREST BROOKLY": "Drink",
    "BAKERY": "Drink",
    "CIAO GLORIA": "Drink",
    "MTA*NYCT PAYGO": "Transportation",
    "CITIBIK": "Transportation",
    "CLAUDE.AI SUBSCRIPTION": "Vince spending",
    "ROGERS": "Monthly fixed cost",
    "CONDO INS": "Monthly fixed cost",
    "GEICO": "Monthly fixed cost",
    "GOOGLE *FI": "Monthly fixed cost",
    "BLUE CROSS": "Health & Wellness",
    "ALOHI": "Monthly fixed cost"
}
create_and_insert(db_name, table_name, category_map)
