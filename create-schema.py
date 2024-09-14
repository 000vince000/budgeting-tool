import duckdb
import os
import db_operations

def create_table_with_sequence(conn, table_name, columns, unique_index_columns=None):
    create_sequence(conn, table_name)
    create_table(conn, table_name, columns)
    if unique_index_columns:
        create_unique_index(conn, table_name, unique_index_columns)

# Then use it like this:
def create_table_consolidated_transactions(conn):
    table_name = "consolidated_transactions"
    columns = [
        f"id BIGINT DEFAULT nextval('{table_name}_id_seq') PRIMARY KEY",
        '"Card" VARCHAR',
        '"Transaction Date" DATE',
        '"Description" VARCHAR',
        '"Category" VARCHAR',
        '"Type" VARCHAR',
        '"Amount" DECIMAL(10, 2)',
        '"Memo" VARCHAR'
    ]
    create_table_with_sequence(conn, table_name, columns, ["Card", "Transaction Date", "Description", "Amount"])

# Repeat for other table creation functions

def create_sequence(conn, table_name):
    create_seq_query = f"CREATE SEQUENCE IF NOT EXISTS {table_name}_id_seq START 1;"
    try:
        conn.execute(create_seq_query)
        print(f"Sequence {table_name}_id_seq created successfully")
    except Exception as e:
        print(f"An error occurred while creating sequence for {table_name}: {str(e)}")
        raise

def create_unique_index(conn, table_name, columns):
    quoted_columns = ', '.join(f'"{col}"' for col in columns)
    create_unique_index_query = f"""
    CREATE UNIQUE INDEX unique_{table_name} ON {table_name} ({quoted_columns});
    """
    try:
        conn.execute(create_unique_index_query)
        print(f"Unique index unique_{table_name} created successfully")
    except Exception as e:
        print(f"An error occurred while creating unique index for {table_name}: {str(e)}")
        raise

# This function is idempotent
def create_table_category_budgets(conn):
    table_name = "category_budgets"
    create_sequence(conn, table_name)
    columns = [
        f"id BIGINT DEFAULT nextval('{table_name}_id_seq') PRIMARY KEY",
        "category VARCHAR",
        "budget INTEGER",
        "timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        f"FOREIGN KEY (category) REFERENCES categories(category)"
    ]
    create_table_with_sequence(conn, table_name, columns, ["category"])

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
    table_name = "vendor_category_mapping"
    create_sequence(conn, table_name)
    columns = [
        f"id BIGINT DEFAULT nextval('{table_name}_id_seq') PRIMARY KEY",
        "vendor VARCHAR",
        "category VARCHAR",
        f"FOREIGN KEY (category) REFERENCES categories(category)"
    ]
    create_table_with_sequence(conn, table_name, columns, ["vendor"])

def create_table_surplus_and_deficit_breakdowns_and_items(conn):
    table_name = "surplus_and_deficit_breakdowns"
    create_sequence(conn, table_name)
    columns = [
        f"id BIGINT DEFAULT nextval('{table_name}_id_seq') PRIMARY KEY",
        "description VARCHAR",
        "breakdown JSON",
        "effective_date DATE",
        "terminal_date DATE",
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ]
    create_table_with_sequence(conn, table_name, columns, ["description"])
    create_table_surplus_and_deficit_breakdown_items(conn)


def create_table_surplus_and_deficit_breakdown_items(conn):
    table_name = "surplus_and_deficit_breakdown_items"
    create_sequence(conn, table_name)
    columns = [
        f"id BIGINT DEFAULT nextval('{table_name}_id_seq') PRIMARY KEY",
        "surplus_and_deficit_breakdown_id BIGINT",
        "category VARCHAR",
        "description VARCHAR",
        "amount DECIMAL(10, 2)",
        "date DATE",
        "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        f"FOREIGN KEY (surplus_and_deficit_breakdown_id) REFERENCES surplus_and_deficit_breakdowns(id)",
        f"FOREIGN KEY (category) REFERENCES categories(category)"
    ]
    create_table_with_sequence(conn, table_name, columns, ["surplus_and_deficit_breakdown_id", "category", "date"])

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
            create_table_surplus_and_deficit_breakdowns_and_items(conn)
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