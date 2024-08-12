import pandas as pd
import sys
from collections import defaultdict
import duckdb
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
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return None
    except pd.errors.EmptyDataError:
        print(f"Error: File '{input_file}' is empty.")
        return None
    except pd.errors.ParserError:
        print(f"Error: Unable to parse '{input_file}'. Make sure it's a valid CSV file.")
        return None

    # remove payment debit
    df = df[df['Description'] != "AUTOMATIC PAYMENT - THANK"]

    if 'Post Date' in df.columns:
        df = df.drop(columns=['Post Date'])
    else:
        print(f"Warning: 'Post Date' column not found in '{input_file}'. Proceeding without removing it.")

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

    additional_category_map = {
        "Bills & Utilities": "Monthly fixed cost"
    }

    df['Description'] = df['Description'].str.strip()
    
    # First mapping based on Description
    for key, value in category_map.items():
        df.loc[df['Description'].str.contains(key, case=False, na=False), 'Category'] = value

    # Second mapping based on Category
    for key, value in additional_category_map.items():
        df.loc[df['Category'].str.contains(key, case=False, na=False), 'Category'] = value
    
    prompt_count = defaultdict(int)
    
    for index, row in df.iterrows():
        if row['Category'] in ["Professional Services", "Personal"]:
            description = row['Description']
            
            # Check if this description has already been categorized
            category = next((v for k, v in category_map.items() if k in description), None)
            
            if category is None:
                new_category = get_user_choice(global_categories, input_file, description, row['Category'])
                df.at[index, 'Category'] = new_category
                
                prompt_count[description] += 1
                
                # If this description has been prompted more than twice, add it to category_map
                if prompt_count[description] > 1:
                    key = description.split()[0]  # Use the first word of the description as the key
                    category_map[key] = new_category
                    print(f"Memorized: {key} -> {new_category}")
            else:
                df.at[index, 'Category'] = category

    return df
def process_files_parallel(input_files):
    # Predefined categories
    predefined_categories = [
        "Groceries", "Food & Drink", "Travel", "Drink", "Shopping",
        "Automotive", "Health & Wellness", "Monthly fixed cost",
        "Vince spending", "Transportation", "Home", "Entertainment",
        "Education", "Kat spending", "Gas", "Fees & Adjustments",
        "Kids", "Gifts & Donations"
    ]

    # Get all unique categories from all files
    all_categories = set(predefined_categories)
    for file in input_files:
        try:
            df = pd.read_csv(file)
            all_categories.update(df['Category'].dropna().astype(str).unique())
        except Exception as e:
            print(f"Error reading categories from {file}: {e}")

    global_categories = sorted([str(cat) for cat in all_categories if str(cat) not in ["Professional Services", "Personal", "nan"]])

    processed_dfs = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_file = {executor.submit(process_csv, file, global_categories): file for file in input_files}
        for future in concurrent.futures.as_completed(future_to_file):
            file = future_to_file[future]
            try:
                df = future.result()
                if df is not None and not df.empty:
                    processed_dfs.append(df)
                else:
                    print(f"Warning: No valid data processed from '{file}'.")
            except Exception as exc:
                print(f"Error processing '{file}': {exc}")
    
    if not processed_dfs:
        print("Error: No valid data processed from any input files.")
        return None
    
    return pd.concat(processed_dfs, ignore_index=True)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <input_csv_file1> [<input_csv_file2> ...]")
        sys.exit(1)
    
    input_files = sys.argv[1:]
    
    # Process all input files in parallel
    combined_df = process_files_parallel(input_files)

    if combined_df is not None and not combined_df.empty:
        try:
            output_file = 'finance-2024-combined.csv'
            combined_df.to_csv(output_file, index=False)
            print(f"Processing complete. Output saved to {output_file}")
        except PermissionError:
            print("Error: Permission denied when trying to save the file. Make sure you have write access to the current directory.")
        except Exception as e:
            print(f"An unexpected error occurred while saving the file: {str(e)}")
    else:
        print("Error: No data to save. Please check your input files and try again.")
