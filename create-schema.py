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

def create_table_vendor_category_mapping(conn):
    try:
        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS vendor_category_mapping (
            vendor VARCHAR PRIMARY KEY,
            category VARCHAR,
            FOREIGN KEY (category) REFERENCES categories(category)
        )
        """
        conn.execute(create_table_query)
        print("Table vendor_category_mapping created successfully")

    except Exception as e:
        print(f"An error occurred while creating vendor_category_mapping: {str(e)}")
        raise

def create_table_surplus_and_deficit_breakdowns(conn):
    try:
        # Create autoincrementing sequence
        create_seq_query = "CREATE SEQUENCE IF NOT EXISTS surplus_and_deficit_breakdowns_id_seq START 1;"
        conn.execute(create_seq_query)

        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS surplus_and_deficit_breakdowns (
            id BIGINT DEFAULT nextval('surplus_and_deficit_breakdowns_id_seq') PRIMARY KEY,
            description VARCHAR,
            breakdown JSON,
            effective_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        conn.execute(create_table_query)
        print("Table surplus_and_deficit_breakdowns created successfully")
        create_table_surplus_and_deficit_breakdown_items(conn)

    except Exception as e:
        print(f"An error occurred while creating surplus_and_deficit_breakdowns: {str(e)}")
        raise

def create_table_surplus_and_deficit_breakdown_items(conn):
    try:
        # Create autoincrementing sequence
        create_seq_query = "CREATE SEQUENCE IF NOT EXISTS surplus_and_deficit_breakdown_items_id_seq START 1;"
        conn.execute(create_seq_query)

        # Create table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS surplus_and_deficit_breakdown_items (
            id BIGINT DEFAULT nextval('surplus_and_deficit_breakdown_items_id_seq') PRIMARY KEY,
            surplus_and_deficit_breakdown_id BIGINT,
            category VARCHAR,
            description VARCHAR,
            amount DECIMAL(10, 2),
            date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (surplus_and_deficit_breakdown_id) REFERENCES surplus_and_deficit_breakdowns(id),
            FOREIGN KEY (category) REFERENCES categories(category)
        )
        """
        conn.execute(create_table_query)
        print("Table surplus_and_deficit_breakdown_items created successfully")

    except Exception as e:
        print(f"An error occurred while creating surplus_and_deficit_breakdown_items: {str(e)}")
        raise

def create_schema_menu(conn):
    while True:
        print("\nCreate Schema Menu:")
        print("1. Create consolidated_transactions table")
        print("2. Create category_budgets table")
        print("3. Create current_budgets view")
        print("4. Create vendor_category_mapping table")
        print("5. Create surplus_and_deficit_breakdowns table")
        print("6. Exit")
        
        choice = input("Enter your choice (1-6): ")
        
        if choice == '1':
            create_table_consolidated_transactions(conn)
        elif choice == '2':
            create_table_category_budgets(conn)
        elif choice == '3':
            create_current_budgets_view(conn)
        elif choice == '4':
            create_table_vendor_category_mapping(conn)
        elif choice == '5':
            create_table_surplus_and_deficit_breakdowns(conn)
        elif choice == '6':
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    db_name = 'budgeting-tool.db'
    
    try:
        conn = duckdb.connect(db_name)
        create_schema_menu(conn)
        print("Schema creation completed.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close()