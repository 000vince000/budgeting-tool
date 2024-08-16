import pandas as pd
import sys
import os
from collections import defaultdict
import concurrent.futures
import threading

input_lock = threading.Lock()

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

def get_category(description, category_map, unique_categories, user_choices):
    for key, value in category_map.items():
        if key.lower() in description.lower():
            return value, False  # False indicates no user intervention
    
    if description in user_choices:
        return user_choices[description], False  # False because this was a previous choice

    with input_lock:
        print(f"\nTransaction: {description}")
        print("Choose a category or enter a new one:")
        for i, cat in enumerate(unique_categories, 1):
            print(f"{i}. {cat}")
        print(f"{len(unique_categories) + 1}. Enter a new category")
        print(f"{len(unique_categories) + 2}. Exclude this transaction")
        
        while True:
            try:
                choice = int(input("Enter the number of your choice: "))
                if 1 <= choice <= len(unique_categories):
                    user_choices[description] = unique_categories[choice - 1]
                    return unique_categories[choice - 1], True  # True indicates user intervention
                elif choice == len(unique_categories) + 1:
                    new_category = input("Enter the new category: ")
                    user_choices[description] = new_category
                    return new_category, True  # True indicates user intervention
                elif choice == len(unique_categories) + 2:
                    user_choices[description] = "EXCLUDE"
                    return "EXCLUDE", True  # True indicates user intervention
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

def process_chase_csv(input_file, global_categories, user_choices):
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading file '{input_file}': {e}")
        return None

    df = df[df['Description'] != "AUTOMATIC PAYMENT - THANK"]
    df['Card'] = os.path.basename(input_file).split('_')[0]
    df['Memo'] = df.get('Memo', '').fillna('')

    category_map = {
        "Netflix.com": "Entertainment",
        "CUBESMART": "Monthly fixed cost",
        "Patreon": "Vince spending",
        "NYTimes": "Kat spending",
        "AIRALO": "Monthly fixed cost",
        "Spotify USA": "Monthly fixed cost",
        "NEW YORK MAGAZINE": "Kat spending",
        "USAA INSURANCE PAYMENT": "Monthly fixed cost",
        "LYFT ": "Transportation",
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
        "CLAUDE.AI SUBSCRIPTION": "Vince spending"
    }

    rows_to_drop = []

    for index, row in df.iterrows():
        if pd.isna(row['Category']) or row['Category'] in ["Professional Services", "Personal", ""]:
            category = next((v for k, v in category_map.items() if k.lower() in row['Description'].lower()), None)
            if category is None:
                category, user_intervened = get_category(row['Description'], category_map, global_categories, user_choices)
            else:
                user_intervened = False
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

def process_schwab_csv(input_file, global_categories, user_choices):
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

    category_map = {
        'E-ZPASS': 'Transportation',
        'NYC FINANCE PARKING': 'Transportation',
        'CHARLIE CHEN': 'Transportation',
        'GRUBHUB HOLDING': 'Salary',
        'NYCSHININGSMILES NYCSHINING': 'Kids',
        'NAJERA-ESTEBAN': 'Kids',
        'WEB PMTS': 'Monthly property expense',
        'MORTGAGE': 'Monthly mortgage expense',
        'JESSE D VANDENBERGH': 'Rental income',
        'Deposit Mobile Banking': 'Health & Wellness'
    }

    rows_to_drop = []

    for index, row in df.iterrows():
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

def process_files_parallel(input_files, process_func, global_categories, user_choices):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        processed_dfs = list(executor.map(lambda f: process_func(f, global_categories, user_choices), input_files))
    
    processed_dfs = [df for df in processed_dfs if df is not None and not df.empty]
    return pd.concat(processed_dfs, ignore_index=True) if processed_dfs else None

def main():
    global_categories = [
        "Groceries", "Food & Drink", "Travel", "Drink", "Shopping",
        "Automotive", "Health & Wellness", "Monthly fixed cost",
        "Vince spending", "Transportation", "Home", "Entertainment",
        "Education", "Kat spending", "Gas", "Fees & Adjustments",
        "Kids", "Gifts & Donations", "Monthly property expense",
        "Monthly mortgage expense", "Salary", "Rental income"
    ]

    user_choices = {}
    chase_files = []
    schwab_files = []

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
        chase_df = process_files_parallel(chase_files, process_chase_csv, global_categories, user_choices)
        if chase_df is not None:
            combined_df = pd.concat([combined_df, chase_df], ignore_index=True)

    if schwab_files:
        schwab_df = process_files_parallel(schwab_files, process_schwab_csv, global_categories, user_choices)
        if schwab_df is not None:
            combined_df = pd.concat([combined_df, schwab_df], ignore_index=True)

    if not combined_df.empty:
        output_file = 'finance-2024-combined.csv'
        combined_df.to_csv(output_file, index=False)
        print(f"Processing complete. Output saved to {output_file}")

        for category in combined_df['Category'].unique():
            if pd.notna(category):
                category_count = combined_df['Category'].eq(category).sum()
                print(f"\nTransactions marked as '{category}': {category_count} ({category_count / len(combined_df) * 100:.2f}%)")

    else:
        print("Error: No data to save. Please check your input files and try again.")

if __name__ == "__main__":
    main()
