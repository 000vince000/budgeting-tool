import pandas as pd
import sys
import os
from collections import defaultdict
import concurrent.futures
import threading
import duckdb
from datetime import datetime

input_lock = threading.Lock()
debugLevel = None

def get_user_choice(prompt, options):
    while True:
        print(prompt)
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        try:
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(options):
                return options[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def get_input_files(bank_type):
    files = []
    while True:
        file = input(f"Enter path to {bank_type} CSV file (or press Enter if done): ")
        if file == "":
            break
        if os.path.exists(file):
            files.append(file)
        else:
            print("File not found. Please try again.")
    return files

def currency_to_float(x):
    if pd.isna(x):
        return 0.0
    return float(str(x).replace('$', '').replace(',', ''))

# this function prompts user for choice of category
def get_category(description, category_map, unique_categories, user_choices):
    for key, value in category_map.items():
        if key.lower() in description.lower():
            return value, False  # False indicates no user intervention
    
    if description in user_choices:
        return user_choices[description], False  # False because this was a previous choice

    with input_lock:
        print(f"\nTransaction: {description}")
        print("Choose a category or enter a new one:")
        
        # Sort categories alphabetically, excluding "EXCLUDE"
        sorted_categories = sorted([cat for cat in unique_categories if cat != "EXCLUDE"])
        
        # Add "EXCLUDE" option at the end
        sorted_categories.append("EXCLUDE")
        
        for i, cat in enumerate(sorted_categories, 1):
            print(f"{i}. {cat}")
        print(f"{len(sorted_categories) + 1}. Enter a new category")
        
        while True:
            try:
                choice = int(input("Enter the number of your choice: "))
                if 1 <= choice <= len(sorted_categories):
                    selected_category = sorted_categories[choice - 1]
                    user_choices[description] = selected_category
                    return selected_category, True  # True indicates user intervention
                elif choice == len(sorted_categories) + 1:
                    new_category = input("Enter the new category: ")
                    user_choices[description] = new_category
                    return new_category, True  # True indicates user intervention
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

def apply_category_mapping(description, category_map):
    for key, value in category_map.items():
        if key.lower() in description.lower():
            return value
    return None

def process_chase_csv(input_file, global_categories, user_choices, category_map):
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading file '{input_file}': {e}")
        return None

    df = df[df['Description'] != "AUTOMATIC PAYMENT - THANK"]
    df['Card'] = os.path.basename(input_file).split('_')[0]
    df['Memo'] = df.get('Memo', '').fillna('')

    rows_to_drop = []

    for index, row in df.iterrows():
        mapped_category = apply_category_mapping(row['Description'], category_map)
        
        if mapped_category:
            df.at[index, 'Category'] = mapped_category
            df.at[index, 'Memo'] += ' Category assigned automatically via script'
        elif pd.isna(row['Category']) or row['Category'] in ["Professional Services", "Personal", ""]:
            category, user_intervened = get_category(row['Description'], category_map, global_categories, user_choices)
            if category == "EXCLUDE":
                rows_to_drop.append(index)
            else:
                df.at[index, 'Category'] = category
                if user_intervened:
                    df.at[index, 'Memo'] += ' Category assigned by user via script'
                else:
                    df.at[index, 'Memo'] += ' Category assigned automatically via script'
        else:
            # If the category is already defined and valid, keep it
            if row['Category'] not in global_categories:
                global_categories.append(row['Category'])

    df = df.drop(rows_to_drop)
    return df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]

def process_schwab_csv(input_file, global_categories, user_choices, category_map):
    usecols = ['Date', 'Description', 'Type', 'Withdrawal', 'Deposit']
    df = pd.read_csv(input_file, usecols=usecols)
    
    df = df[~df['Description'].str.contains('CHASE CREDIT', case=False, na=False)]
    df = df[df['Type'] != 'TRANSFER']

    df['Withdrawal'] = df['Withdrawal'].apply(currency_to_float)
    df['Deposit'] = df['Deposit'].apply(currency_to_float)

    df['Category'] = ''
    df['Amount'] = df['Deposit'] - df['Withdrawal']
    df['Memo'] = ''
    df['Transaction Date'] = df['Date']

    rows_to_drop = []

    for index, row in df.iterrows():
        mapped_category = apply_category_mapping(row['Description'], category_map)
        
        if mapped_category:
            df.at[index, 'Category'] = mapped_category
            df.at[index, 'Memo'] += ' Category assigned automatically via script'
        else:
            category, user_intervened = get_category(row['Description'], category_map, global_categories, user_choices)
            if category == "EXCLUDE":
                rows_to_drop.append(index)
            else:
                df.at[index, 'Category'] = category
                if user_intervened:
                    df.at[index, 'Memo'] += ' Category assigned by user via script'
                else:
                    df.at[index, 'Memo'] += ' Category assigned automatically via script'

    df = df.drop(rows_to_drop)
    df['Card'] = 'Schwab'

    return df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]

def process_files_parallel(input_files, process_func, global_categories, user_choices, category_map):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        processed_dfs = list(executor.map(lambda f: process_func(f, global_categories, user_choices, category_map), input_files))
    
    processed_dfs = [df for df in processed_dfs if df is not None and not df.empty]
    return pd.concat(processed_dfs, ignore_index=True) if processed_dfs else None

# This function queries and load category map from duckdb
def get_category_mapping_from_db(conn):
    query = f"""
        select keyword, category from category_matching_patterns
    """
    # Execute the query and fetch the results as a dictionary
    data = conn.execute(query).fetchall()
    return dict(data)

# This function queries the duckdb for existing categories
def get_global_categories_from_db(conn):
    query = f"""
        select category from categories
    """
    # Execute the query and fetch the results as a list
    data = conn.execute(query).fetchall()
    return data

# This function persists df into duckdb
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

def main():
    user_choices = {}
    chase_files = []
    schwab_files = []

    conn = duckdb.connect("budgeting-tool.db")
    category_map = get_category_mapping_from_db(conn)
    global_categories = get_global_categories_from_db(conn)
    table_name = 'consolidated_transactions'

    while True:
        bank_choice = get_user_choice("Select bank type:", ["Chase", "Charles Schwab", "Done"])
        if bank_choice == "Done":
            break
        elif bank_choice == "Chase":
            chase_files.extend(get_input_files("Chase"))
        elif bank_choice == "Charles Schwab":
            schwab_files.extend(get_input_files("Charles Schwab"))

    combined_df = pd.DataFrame()

    if chase_files:
        chase_df = process_files_parallel(chase_files, process_chase_csv, global_categories, user_choices, category_map)
        if chase_df is not None:
            combined_df = pd.concat([combined_df, chase_df], ignore_index=True)

    if schwab_files:
        schwab_df = process_files_parallel(schwab_files, process_schwab_csv, global_categories, user_choices, category_map)
        if schwab_df is not None:
            combined_df = pd.concat([combined_df, schwab_df], ignore_index=True)

    if not combined_df.empty:
        #output_file = 'finance-2024-combined.csv'
        #combined_df.to_csv(output_file, index=False)
        #print(f"Processing complete. Output saved to {output_file}")
        persist_data_in_db(conn, combined_df, table_name)

    else:
        print("Error: No data to save. Please check your input files and try again.")

    # Close the connection
    conn.close()
if __name__ == "__main__":
    main()
