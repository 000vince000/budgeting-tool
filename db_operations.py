import duckdb
from datetime import datetime
import pandas as pd

def get_db_connection(db_name):
    conn = duckdb.connect(db_name)
    return conn

def query_and_return_df(conn, query_statement, params=None):
    if params:
        result = conn.execute(query_statement, params)
    else:
        result = conn.execute(query_statement)
    return result.df()

def get_category_mapping_from_db(conn):
    query = """
        select keyword, category from category_matching_patterns
    """
    data = conn.execute(query).fetchall()
    return dict(data)

def get_global_categories_from_db(conn):
    query = """
        select category from categories
    """
    data = conn.execute(query).fetchall()
    return [item[0] for item in data]

# TODO: refactor this to be more specific rather than generic
def persist_data_in_db(conn, df, quoted_table_name):
    cols = df.columns.to_list()
    imploded_col_names = ', '.join(f'"{col}"' for col in cols)
    inserted_count = 0
    error_count = 0

    for index, row in df.iterrows():
        insert_query = f"""
        INSERT INTO {quoted_table_name} ({imploded_col_names}) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        try:
            conn.execute(insert_query, (
                row[cols[0]],
                datetime.strptime(row[cols[1]], '%m/%d/%Y').date(),
                row[cols[2]],
                row[cols[3]],
                row[cols[4]],
                row[cols[5]],
                row[cols[6]]
            ))
            inserted_count += 1
        except duckdb.ConstraintException as ce:
            print(f"ConstraintException for row {index}: {str(ce)}")
            error_count += 1
        except Exception as e:
            print(f"Error inserting row {index}: {str(e)}")
            error_count += 1
    
    print(f"Insertion complete. Rows inserted: {inserted_count}, Rows failed: {error_count}")

def insert_category_budget(conn, category, budget):
    try:
        insert_query = """
        INSERT INTO category_budgets (category, budget)
        VALUES (?, ?)
        """
        conn.execute(insert_query, (category, budget))
        conn.commit()
        print(f"Budget of {budget} for category '{category}' inserted successfully")
    except Exception as e:
        print(f"An error occurred while inserting budget for category '{category}': {str(e)}")
        conn.rollback()
        raise

def recategorize_transaction(conn, transaction_id, new_category, old_category):
    query = """
    UPDATE consolidated_transactions
    SET Category = ?,
        Memo = CASE
            WHEN Memo IS NULL OR Memo = '' THEN ?
            ELSE Memo || ?
        END
    WHERE id = ?
    """
    memo_addition = f". Recategorized by user from {old_category}"
    if new_category is None:
        memo_addition += ". Set to NULL by user from {old_category}"
    conn.execute(query, (new_category, memo_addition, memo_addition, transaction_id))
    conn.commit()

def get_latest_month(conn):
    query = """
    SELECT DATE_TRUNC('month', MAX("Transaction Date")) AS month
    FROM consolidated_transactions
    """
    return conn.execute(query).fetchone()[0]

def fetch_transactions(conn, category, year, month):
    query = """
    SELECT id, "Transaction Date", Description, Amount, Category
    FROM consolidated_transactions
    WHERE Category = ?
      AND strftime('%Y', "Transaction Date") = ?
      AND strftime('%m', "Transaction Date") = ?
    ORDER BY "Transaction Date" DESC
    """
    return query_and_return_df(conn, query, [category, str(year), str(month).zfill(2)])

def show_p95_expensive_nonrecurring_for_latest_month(conn, year, month):
    query = """
    WITH specified_month AS (
        SELECT MAKE_DATE(?, ?, 1) AS month
    ),
    nonrecurring_expenses AS (
        SELECT Description, Amount, "Transaction Date", Category
        FROM consolidated_transactions, specified_month
        WHERE Category NOT IN ('Monthly fixed cost', 'Monthly property expense', 'Monthly mortgage expense')
          AND Amount < 0
          AND DATE_TRUNC('month', "Transaction Date") = specified_month.month
    ),
    percentile_calc AS (
        SELECT *, 
               PERCENT_RANK() OVER (ORDER BY Amount DESC) AS percentile
        FROM nonrecurring_expenses
    )
    SELECT Description, Amount, "Transaction Date", Category
    FROM percentile_calc
    WHERE percentile >= 0.95
    ORDER BY Amount ASC
    """
    df = query_and_return_df(conn, query, [year, month])
    
    if df.empty:
        return None
    else:
        df['Amount'] = df['Amount'].abs()  # Convert to positive for display
        df = df.sort_values('Amount', ascending=False)  # Sort by Amount in descending order        
        return df

def insert_adjustment_transaction(conn, transaction_date, description, amount, category):
    query = """
    INSERT INTO consolidated_transactions ("Transaction Date", Description, Amount, Category)
    VALUES (?, ?, ?, ?)
    """
    conn.execute(query, (transaction_date, description, amount, category))
    conn.commit()

def get_month_summary(conn, year, month):
    with open('specific-month-summary.sql', 'r') as file:
        query = file.read()
    
    return query_and_return_df(conn, query, [year, month])

def insert_vendor_category_mapping(conn, vendor, category):
    # Remove the transaction handling from this function
    category_check_query = "SELECT COUNT(*) FROM categories WHERE category = ?"
    result = conn.execute(category_check_query, [category]).fetchone()
    if result[0] == 0:
        raise ValueError(f"Category '{category}' does not exist in the categories table.")

    print(f"DEBUG: Inserting vendor-category mapping: '{vendor}' -> '{category}'")
    insert_query = """
    INSERT INTO vendor_category_mapping (vendor, category)
    VALUES (?, ?)
    """
    conn.execute(insert_query, [vendor, category])
    print(f"Vendor '{vendor}' successfully mapped to category '{category}'")

def get_transactions_by_vendor(conn, vendor):
    query = """
    SELECT id, "Transaction Date", Description, Amount, Category
    FROM consolidated_transactions
    WHERE Description LIKE ?
    ORDER BY "Transaction Date" DESC
    """
    
    # Use wildcards to match partial vendor names in the description
    vendor_pattern = f"%{vendor}%"
    
    return query_and_return_df(conn, query, [vendor_pattern])

def recategorize_transactions(conn, transaction_ids, new_category):
    # Remove the transaction handling from this function
    query = """
    UPDATE consolidated_transactions
    SET Category = ?,
        Memo = CASE
            WHEN Memo IS NULL OR Memo = '' THEN ?
            ELSE Memo || ?
        END
    WHERE id = ?
    """

    for transaction_id in transaction_ids:
        old_category_query = "SELECT Category FROM consolidated_transactions WHERE id = ?"
        old_category = conn.execute(old_category_query, [transaction_id]).fetchone()[0]

        memo_addition = f". Recategorized by user from {old_category}"
        if new_category is None:
            memo_addition += f". Set to NULL by user from {old_category}"

        conn.execute(query, (new_category, memo_addition, memo_addition, transaction_id))

    print(f"Successfully recategorized {len(transaction_ids)} transactions to '{new_category}'")

def get_vendor_category_mapping(conn, vendor):
    """
    Retrieve the category mapping for a given vendor.

    Args:
    conn (duckdb.DuckDBPyConnection): The database connection.
    vendor (str): The vendor name to look up.

    Returns:
    str or None: The category associated with the vendor, or None if no mapping exists.
    """
    try:
        query = """
        SELECT category 
        FROM vendor_category_mapping 
        WHERE vendor = ?
        """
        result = conn.execute(query, [vendor]).fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"An error occurred while retrieving vendor-category mapping: {str(e)}")
        return None

