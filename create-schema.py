import duckdb

def create_schema(db_name, table_name):
    conn = duckdb.connect(db_name)
    quoted_table_name = f'"{table_name}"'
    
    try:
        # Create autoincrementing sequence
        create_seq_query = "CREATE SEQUENCE consolidated_transactions_id_seq START 1;"
        conn.execute(create_seq_query)

        # Create table
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {quoted_table_name} (
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
        print(f"Table {quoted_table_name} created successfully")

        conn.commit()
    except Exception as e:
        print(f"An error occurred while creating the schema: {str(e)}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    db_name = 'budgeting-tool.db'
    table_name = 'consolidated_transactions'
    create_schema(db_name, table_name)