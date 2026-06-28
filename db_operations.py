import os
import duckdb
from datetime import datetime, date
import pandas as pd
import warnings

# Suppress specific UserWarning
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy connectable")

def get_db_connection(db_name):
    conn = duckdb.connect(db_name)
    return conn

def execute_query(conn, query, params=None):
    try:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        return result
    except Exception as e:
        print(f"Error executing query: {query}, params: {params}")
        print(f"Error message: {str(e)}")
        raise

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
    data = execute_query(conn, query).fetchall()
    return dict(data)

def get_vendor_mapping_from_db(conn):
    query = """
        select vendor, category from vendor_category_mapping
    """
    data = execute_query(conn, query).fetchall()
    return dict(data)

def get_global_categories_from_db(conn):
    query = """
        select category from categories
    """
    data = execute_query(conn, query).fetchall()
    return [item[0] for item in data]

def get_categories_with_groups_from_db(conn):
    query = """
        select category, category_group from categories
    """
    # Just return the raw query results
    return execute_query(conn, query).fetchall()

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
            #print(f"ConstraintException for row {index}: {str(ce)}")
            print(f"Duplicate entry rejected for row {index}, Transaction Date: {row[cols[1]]}, Description: {row[cols[2]]}, Amount: {row[cols[3]]}")
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
        execute_query(conn, insert_query, [category, budget])
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
        memo_addition += f". Set to NULL by user from {old_category}"
    execute_query(conn, query, [new_category, memo_addition, memo_addition, transaction_id])
    conn.commit()

def get_latest_month(conn):
    query = """
    SELECT DATE_TRUNC('month', MAX("Transaction Date")) AS month
    FROM consolidated_transactions
    """
    return execute_scalar_query(conn, query)

def fetch_transactions_by_category(conn, category, year, month):
    query = """
    SELECT id, Card, "Transaction Date", Description, Amount, Category, memo
    FROM consolidated_transactions
    WHERE Category = ?
      AND EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
    ORDER BY "Transaction Date" DESC
    """
    return query_and_return_df(conn, query, [category, year, month])

def fetch_transactions_by_categories(conn, categories, year, month):
    # Create a comma-separated string of quoted category names
    # This is safer than dynamic placeholders for IN clauses in DuckDB
    category_values = ", ".join(f"'{category}'" for category in categories)
    
    query = f"""
    SELECT id, Card, "Transaction Date", Description, Amount, Category, memo
    FROM consolidated_transactions
    WHERE Category IN ({category_values})
      AND EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
    ORDER BY category, amount asc
    """
    return query_and_return_df(conn, query, [year, month])
    
def get_biggest_oneoff_expenses(conn, year, month):
    query = """
    WITH specified_month AS (
        SELECT MAKE_DATE(?, ?, 1) AS month
    ),
    nonrecurring_expenses AS (
        SELECT t1.Description, t1.Amount, t1."Transaction Date", t1.Category
        FROM consolidated_transactions t1, specified_month
        WHERE t1.Category NOT IN ('Monthly fixed cost', 'Monthly property expense', 'Monthly mortgage expense')
          AND t1.Amount < 0
          AND DATE_TRUNC('month', t1."Transaction Date") = specified_month.month
          AND NOT EXISTS (
            SELECT 1 
            FROM consolidated_transactions t2
            WHERE t2.Description = t1.Description 
              AND ABS(t2.Amount) BETWEEN ABS(t1.Amount) * 0.95 AND ABS(t1.Amount) * 1.05
              AND DATE_TRUNC('month', t2."Transaction Date") != DATE_TRUNC('month', t1."Transaction Date")
          )
    ),
    percentile_calc AS (
        SELECT *, 
               PERCENT_RANK() OVER (ORDER BY Amount DESC) AS percentile
        FROM nonrecurring_expenses
    )
    SELECT Description, Amount, "Transaction Date", Category
    FROM percentile_calc
    WHERE percentile >= 0.85
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
    execute_query(conn, query, (transaction_date, description, amount, category))
    conn.commit()

def get_month_summary(conn, year, month):
    sql_path = os.path.join(os.path.dirname(__file__), 'specific-month-summary.sql')
    with open(sql_path, 'r') as file:
        query = file.read()
    
    return query_and_return_df(conn, query, [year, month])

def insert_vendor_category_mapping(conn, vendor, category):
    category_check_query = "SELECT COUNT(*) FROM categories WHERE category = ?"
    result = execute_query(conn, category_check_query, [category]).fetchone()
    if result[0] == 0:
        raise ValueError(f"Category '{category}' does not exist in the categories table.")

    insert_query = """
    INSERT INTO vendor_category_mapping (vendor, category)
    VALUES (?, ?)
    ON CONFLICT (vendor) DO UPDATE SET category = EXCLUDED.category
    """
    execute_query(conn, insert_query, [vendor, category])
    print(f"Vendor '{vendor}' successfully mapped to category '{category}'")

def delete_vendor_category_mapping(conn, vendor):
    query = """
    DELETE FROM vendor_category_mapping
    WHERE vendor = ?
    """
    execute_query(conn, query, [vendor])

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
        result = execute_query(conn, old_category_query, [transaction_id])
        old_category = result.fetchone()
        if old_category is None:
            print(f"WARNING: No transaction found with id {transaction_id}")
            continue
        old_category = old_category[0]
        memo_addition = f". Recategorized by user from {old_category}"
        if new_category is None:
            memo_addition += f". Set to NULL by user from {old_category}"

        execute_query(conn, query, (new_category, memo_addition, memo_addition, transaction_id))

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
        result = execute_query(conn, query, [vendor]).fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"An error occurred while retrieving vendor-category mapping: {str(e)}")
        return None

def insert_surplus_deficit_breakdown(conn, description, breakdown, effective_date):
    try:
        query = """
        INSERT INTO surplus_and_deficit_breakdowns (description, breakdown, effective_date)
        VALUES (?, ?, ?)
        RETURNING id
        """
        result = execute_query(conn, query, [description, breakdown, effective_date]).fetchone()
        conn.commit()
        print("Surplus/Deficit Breakdown inserted successfully.")
        return result[0]  # Return the id of the inserted row
    except Exception as e:
        conn.rollback()
        print(f"An error occurred while inserting Surplus/Deficit Breakdown: {str(e)}")
        raise

def get_subtotal_by_category_group_for_month(conn, year, month):
    query = """
    SELECT category_group, SUM(t.amount) as subtotal
    FROM consolidated_transactions t
    JOIN categories c using (category)
    WHERE EXTRACT(YEAR FROM t."Transaction Date") = ?
    AND EXTRACT(MONTH FROM t."Transaction Date") = ?
    AND category is not null
    GROUP BY 1
    """
    return query_and_return_df(conn, query, [year, month])

def get_category_group_summary_with_percentiles(conn, year, month):
    query = """
    WITH monthly_group_totals AS (
        SELECT
            c.category_group,
            DATE_TRUNC('month', t."Transaction Date") AS month,
            SUM(t.amount) AS monthly_total
        FROM consolidated_transactions t
        JOIN categories c USING (category)
        WHERE category IS NOT NULL
        GROUP BY 1, 2
    ),
    group_stats AS (
        SELECT
            category_group,
            ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ABS(monthly_total))
                  FILTER (WHERE (category_group != 'Revenue' AND monthly_total < 0)
                             OR (category_group  = 'Revenue' AND monthly_total > 0)), 2) AS p50,
            ROUND(PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY ABS(monthly_total))
                  FILTER (WHERE (category_group != 'Revenue' AND monthly_total < 0)
                             OR (category_group  = 'Revenue' AND monthly_total > 0)), 2) AS p85
        FROM monthly_group_totals
        GROUP BY 1
    ),
    current_month AS (
        SELECT
            c.category_group,
            SUM(t.amount) AS subtotal
        FROM consolidated_transactions t
        JOIN categories c USING (category)
        WHERE EXTRACT(YEAR FROM t."Transaction Date") = ?
        AND EXTRACT(MONTH FROM t."Transaction Date") = ?
        AND category IS NOT NULL
        GROUP BY 1
    )
    SELECT
        cm.category_group,
        COALESCE(cm.subtotal, 0) AS subtotal,
        COALESCE(gs.p50, 0) AS p50,
        COALESCE(gs.p85, 0) AS p85
    FROM current_month cm
    LEFT JOIN group_stats gs USING (category_group)
    """
    return query_and_return_df(conn, query, [year, month])

def get_net_income_for_month(conn, year, month):
    query = """
    SELECT SUM(amount) as net_income
    FROM consolidated_transactions
    WHERE EXTRACT(YEAR FROM "Transaction Date") = ?
    AND EXTRACT(MONTH FROM "Transaction Date") = ?
    AND category is not null
    """
    result = execute_query(conn, query, [year, month]).fetchone()
    return result[0] if result[0] is not None else 0

def insert_surplus_deficit_breakdown_item(conn, breakdown_id, category, description, amount, date):
    try:
        query = """
        INSERT INTO surplus_and_deficit_breakdown_items 
        (surplus_and_deficit_breakdown_id, category, description, amount, date)
        VALUES (?, ?, ?, ?, ?)
        """
        execute_query(conn, query, [breakdown_id, category, description, amount, date])
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"An error occurred while inserting Surplus/Deficit Breakdown Item: {str(e)}")
        raise

def get_latest_transaction_date(conn):
    query = """
    SELECT MAX("Transaction Date") 
    FROM consolidated_transactions
    """
    return execute_scalar_query(conn, query) or date.today()

def execute_scalar_query(conn, query, params=None):
    result = conn.execute(query, params).fetchone()
    return result[0] if result else None

def get_p85_for_category(conn, category, year, month):
    query = """
    SELECT PERCENTILE_CONT(0.85) WITHIN GROUP (ORDER BY ABS(Amount))
    FROM consolidated_transactions
    WHERE Category = ?
      AND DATE_TRUNC('month', "Transaction Date") != MAKE_DATE(?, ?, 1)
      AND Amount < 0
    """
    return execute_scalar_query(conn, query, [category, year, month])

def get_transactions_above_threshold(conn, category, year, month, threshold):
    query = """
    SELECT "Transaction Date", Description, Amount
    FROM consolidated_transactions
    WHERE Category = ?
      AND EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
      AND ABS(Amount) > ?
    ORDER BY ABS(Amount) DESC
    """
    return query_and_return_df(conn, query, [category, year, month, threshold])

def get_p90_across_categories(conn, year, month, excluded_categories):
    exclusion_clause = ""
    if excluded_categories:
        excluded_values = ", ".join(f"'{category}'" for category in excluded_categories)
        exclusion_clause = f"AND Category NOT IN ({excluded_values})"

    query = f"""
    SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY ABS(Amount))
    FROM consolidated_transactions
    WHERE EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
      AND Amount < 0
      {exclusion_clause}
    """
    return execute_scalar_query(conn, query, [year, month])

def check_recurring_transaction(conn, description, amount, transaction_date):
    query = """
    SELECT COUNT(*)
    FROM consolidated_transactions
    WHERE Description = ?
      AND ABS(Amount) = ABS(?)
      AND DATE_TRUNC('month', "Transaction Date") != DATE_TRUNC('month', ?)
    """
    return execute_scalar_query(conn, query, [description, amount, transaction_date])

def get_active_breakdowns(conn, year, month):
    query = """
    SELECT id, description, breakdown
    FROM surplus_and_deficit_breakdowns
    WHERE effective_date <= make_date(?, ?, 1)
      AND (terminal_date IS NULL OR terminal_date >= make_date(?, ?, 1))
    """
    return query_and_return_df(conn, query, [year, month, year, month])

def get_breakdown_items_by_date(conn, year, month):
    query = """
    SELECT category, description
    FROM surplus_and_deficit_breakdown_items
    WHERE EXTRACT(YEAR FROM date) = ? AND EXTRACT(MONTH FROM date) = ?
    """
    return query_and_return_df(conn, query, [year, month])

def get_breakdown_items(conn, year, month):
    query = """
    WITH latest_amounts AS (
        SELECT description, amount AS latest_amount, 
               ROW_NUMBER() OVER (PARTITION BY description ORDER BY date DESC) AS rn
        FROM surplus_and_deficit_breakdown_items
        WHERE surplus_and_deficit_breakdown_id IN (
            SELECT id
            FROM surplus_and_deficit_breakdowns
            WHERE make_date(?, ?, 1) between effective_date and coalesce(terminal_date, '2099-01-01')
        )
    )
    SELECT sdi.description, 
           SUM(sdi.amount) as accumulation,
           la.latest_amount
    FROM surplus_and_deficit_breakdown_items sdi
    JOIN latest_amounts la ON sdi.description = la.description AND la.rn = 1
    WHERE sdi.surplus_and_deficit_breakdown_id IN (
        SELECT id
        FROM surplus_and_deficit_breakdowns
        WHERE make_date(?, ?, 1) between effective_date and coalesce(terminal_date, '2099-01-01')
        AND make_date(?, ?, 1) >= sdi.date
    )
    GROUP BY sdi.description, la.latest_amount
    """
    return query_and_return_df(conn, query, [year, month, year, month, year, month])

def get_actual_spending(conn, year, month):
    query = """
    SELECT Category, SUM(Amount) as actual_amount
    FROM consolidated_transactions
    WHERE EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
    GROUP BY Category
    """
    return query_and_return_df(conn, query, [year, month])

def get_goals_and_breakdown_items(conn, year, month):
    query = """
    SELECT category, description, amount
    FROM surplus_and_deficit_breakdown_items
    WHERE date = make_date(?, ?, 1)
    """
    return query_and_return_df(conn, query, [year, month])

def update_transaction_amount(conn, transaction_id, new_amount):
    query = """
    UPDATE consolidated_transactions
    SET Amount = ?
    WHERE id = ?
    """
    execute_query(conn, query, (new_amount, transaction_id))

def update_transaction_memo(conn, transaction_id, new_memo):
    query = """
    UPDATE consolidated_transactions
    SET Memo = ?
    WHERE id = ?
    """
    execute_query(conn, query, (new_memo, transaction_id))

def get_next_sequence_value(conn, sequence_name):
    query = f"SELECT nextval('{sequence_name}')"
    result = conn.execute(query).fetchone()
    return result[0] if result else None

def insert_amortized_transaction(conn, transaction_id, card, transaction_date, description, category, amount, memo):
    query = """
    INSERT INTO consolidated_transactions (id, card, "Transaction Date", Description, Category, Amount, Memo)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    execute_query(conn, query, (transaction_id, card, transaction_date, description, category, amount, memo))
    return transaction_id

def flag_transaction(conn, transaction_id):
    query = """
    INSERT INTO flagged_transactions (transaction_id)
    VALUES (?)
    """
    execute_query(conn, query, [transaction_id])  # Pass transaction_id as a list

def get_flagged_transactions(conn):
    query = """
    SELECT id, "Transaction Date", Description, Amount, Category, Memo
    FROM consolidated_transactions t
    JOIN flagged_transactions f on t.id = f.transaction_id
    """
    return query_and_return_df(conn, query)

def unflag_transaction(conn, transaction_id):
    query = """
    DELETE FROM flagged_transactions
    WHERE transaction_id = ?
    """
    execute_query(conn, query, [transaction_id])

def add_memo_to_transaction(conn, transaction_id, new_memo):
    query = """
    UPDATE consolidated_transactions
    SET Memo = Memo || ?
    WHERE id = ?
    """
    execute_query(conn, query, (new_memo, transaction_id))

def update_transaction_date(conn, transaction_id, old_date, new_date):
    memo_addition = f". Date moved by user from {old_date}"
    query = """
    UPDATE consolidated_transactions
    SET "Transaction Date" = ?,
        Memo = CASE
            WHEN Memo IS NULL OR Memo = '' THEN ?
            ELSE Memo || ?
        END
    WHERE id = ?
    """
    try:
        execute_query(conn, query, (new_date, memo_addition, memo_addition, transaction_id))
        conn.commit()
        return True
    except Exception as e:
        if "ConstraintException" in type(e).__name__ or "constraint" in str(e).lower():
            return False
        raise

def search_transactions_by_keyword(conn, keyword, year, month):
    # Add wildcards to the keyword parameter value rather than embedding ? in quotes
    search_pattern = f"%{keyword}%"
    
    query = """
    SELECT id, "Transaction Date", Description, Amount, Category, Memo
    FROM consolidated_transactions
    WHERE (Description LIKE ? OR Memo LIKE ?)
      AND EXTRACT(YEAR FROM "Transaction Date") = ?
      AND EXTRACT(MONTH FROM "Transaction Date") = ?
    ORDER BY "Transaction Date" DESC
    """
    return query_and_return_df(conn, query, [search_pattern, search_pattern, year, month])
