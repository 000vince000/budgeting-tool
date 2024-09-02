import duckdb
import os

# This function is idempotent
def create_table_consolidated_transactions(conn):
    try:
        # Create autoincrementing sequence
        create_seq_query = "CREATE SEQUENCE IF NOT EXISTS consolidated_transactions_id_seq START 1;"
        conn.execute(create_seq_query)

        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS consolidated_transactions (
            id BIGINT DEFAULT nextval('consolidated_transactions_id_seq') PRIMARY KEY,
            "Card" VARCHAR,
            "Transaction Date" DATE,
            "Description" VARCHAR,
            "Category" VARCHAR,
            "Type" VARCHAR,
            "Amount" DECIMAL(10, 2),
            "Memo" VARCHAR,
            UNIQUE ("Card", "Transaction Date", "Description", "Amount")
        )
        """
        conn.execute(create_table_query)
        print("Table consolidated_transactions created successfully")

    except Exception as e:
        print(f"An error occurred while creating consolidated_transactions: {str(e)}")
        raise

# This function is idempotent
def create_table_category_budgets(conn):
    try:
        # Create autoincrementing sequence
        create_seq_query = "CREATE SEQUENCE IF NOT EXISTS category_budgets_id_seq START 1;"
        conn.execute(create_seq_query)

        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS category_budgets (
            id BIGINT DEFAULT nextval('category_budgets_id_seq') PRIMARY KEY,
            category VARCHAR,
            budget INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category) REFERENCES categories(category)
        )
        """
        conn.execute(create_table_query)
        print("Table category_budgets created successfully")

    except Exception as e:
        print(f"An error occurred while creating category_budgets: {str(e)}")
        raise

def create_current_budgets_view(conn):
    try:
        # Read the SQL query from the file
        current_dir = os.path.dirname(os.path.abspath(__file__))
        sql_file_path = os.path.join(current_dir, 'current-budgets.sql')
        
        with open(sql_file_path, 'r') as sql_file:
            view_query = sql_file.read()

        # Create the view
        create_view_query = f"CREATE OR REPLACE VIEW current_budgets AS {view_query}"
        conn.execute(create_view_query)
        print("View current_budgets created successfully")

    except Exception as e:
        print(f"An error occurred while creating the current_budgets view: {str(e)}")
        raise

if __name__ == "__main__":
    db_name = 'budgeting-tool.db'
    
    try:
        conn = duckdb.connect(db_name)
        create_table_consolidated_transactions(conn)
        create_table_category_budgets(conn)
        create_current_budgets_view(conn)
        conn.commit()
        print("All operations completed successfully")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()