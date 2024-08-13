import pandas as pd
import sys
import os
from collections import defaultdict
import concurrent.futures
import threading

input_lock = threading.Lock()

def get_user_choice(unique_categories, input_file, description, current_category):
    with input_lock:
        print(f"\nFile: {input_file}")
        print(f"Transaction: {description}")
        print("Current category:", current_category)
        print("Choose a new category:")
        for i, cat in enumerate(unique_categories, 1):
            print(f"{i}. {cat}")
        
        while True:
            try:
                choice = int(input("Enter the number of your choice: "))
                if 1 <= choice <= len(unique_categories):
                    return unique_categories[choice - 1]
                else:
                    print("Invalid choice. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number.")

def process_csv(input_file, global_categories):
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

    for key, value in category_map.items():
        mask = df['Description'].str.contains(key, case=False, na=False)
        df.loc[mask, 'Category'] = value
        df.loc[mask, 'Memo'] += ' Category replaced via script'

    for index, row in df.iterrows():
        if pd.isna(row['Category']) or row['Category'] in ["Professional Services", "Personal", ""]:
            category = next((v for k, v in category_map.items() if k.lower() in row['Description'].lower()), None)
            if category is None:
                category = get_user_choice(global_categories, input_file, row['Description'], row['Category'])
                df.at[index, 'Memo'] += ' Category replaced by user via script'
            else:
                df.at[index, 'Memo'] += ' Category replaced by memory via script'
            df.at[index, 'Category'] = category

    return df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]

def process_files_parallel(input_files):
    predefined_categories = [
        "Groceries", "Food & Drink", "Travel", "Drink", "Shopping",
        "Automotive", "Health & Wellness", "Monthly fixed cost",
        "Vince spending", "Transportation", "Home", "Entertainment",
        "Education", "Kat spending", "Gas", "Fees & Adjustments",
        "Kids", "Gifts & Donations"
    ]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        processed_dfs = list(executor.map(lambda f: process_csv(f, predefined_categories), input_files))
    
    # Filter out None values and empty DataFrames
    processed_dfs = [df for df in processed_dfs if df is not None and not df.empty]

    return pd.concat(processed_dfs, ignore_index=True) if processed_dfs else None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_csv_file1> [<input_csv_file2> ...]")
        sys.exit(1)

    combined_df = process_files_parallel(sys.argv[1:])

    if combined_df is not None and not combined_df.empty:
        try:
            output_file = 'finance-2024-combined.csv'
            combined_df.to_csv(output_file, index=False)
            print(f"Processing complete. Output saved to {output_file}")

            for category in combined_df['Category'].unique():
                if pd.notna(category):
                    category_count = combined_df['Category'].eq(category).sum()
                    print(f"\nTransactions marked as '{category}': {category_count} ({category_count / len(combined_df) * 100:.2f}%)")
        except Exception as e:
            print(f"An error occurred while saving the file: {str(e)}")
    else:
        print("Error: No data to save. Please check your input files and try again.")
