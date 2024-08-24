import duckdb
import pandas as pd
from datetime import datetime

def debug_create_and_insert(db_name, table_name, csv_file):
    conn = duckdb.connect(db_name)
    
    # Quote the table name to handle special characters and numbers
    quoted_table_name = f'"{table_name}"'
    
    try:
        # Step 1: Read CSV
        print("Step 1: Reading CSV file")
        df = pd.read_csv(csv_file)
        print(f"CSV loaded. Total rows: {len(df)}")

        # Convert 'Transaction Date' to datetime
        df['Transaction Date'] = pd.to_datetime(df['Transaction Date'], format='%m/%d/%Y')

        # Step 2: Create table
        print("\nStep 2: Creating table")
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table_name} (
            "Card" VARCHAR,
            "Transaction Date" DATE,
            "Description" VARCHAR,
            "Category" VARCHAR,
            "Type" VARCHAR,
            "Amount" DECIMAL(10, 2),
            "Memo" VARCHAR,
            PRIMARY KEY ("Card", "Transaction Date", "Description", "Amount")
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

        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {quoted_table_name} VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            try:
                conn.execute(insert_query, (
                    row['Card'],
                    row['Transaction Date'].strftime('%Y-%m-%d'),  # Convert to YYYY-MM-DD
                    row['Description'],
                    row['Category'],
                    row['Type'],
                    row['Amount'],
                    row['Memo']
                ))
                inserted_count += 1
            except duckdb.ConstraintException as ce:
                print(f"ConstraintException for row {index}: {str(ce)}")
                error_count += 1
            except Exception as e:
                print(f"Error inserting row {index}: {str(e)}")
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
table_name = 'consolidated_transactions'
csv_file = 'finance-2024-combined.csv'

debug_create_and_insert(db_name, table_name, csv_file)
