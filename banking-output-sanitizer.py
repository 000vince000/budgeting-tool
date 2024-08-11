import pandas as pd
import sys
from collections import defaultdict
import duckdb

def get_user_choice(unique_categories):
    while True:
        try:
            for i, cat in enumerate(unique_categories, 1):
                print(f"{i}. {cat}")
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(unique_categories):
                return unique_categories[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def process_csv(input_file):
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")
        return
    except pd.errors.EmptyDataError:
        print(f"Error: File '{input_file}' is empty.")
        return
    except pd.errors.ParserError:
        print(f"Error: Unable to parse '{input_file}'. Make sure it's a valid CSV file.")
        return

    # remove payment debit
    df = df[df['Description'] != "AUTOMATIC PAYMENT - THANK"]

    if 'Post Date' in df.columns:
        df = df.drop(columns=['Post Date'])
    else:
        print("Warning: 'Post Date' column not found. Proceeding without removing it.")

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

    # User prompt for specific categories
    unique_categories = df['Category'].unique().tolist()
    unique_categories = [cat for cat in unique_categories if cat not in ["Professional Services", "Personal"]]
    
    prompt_count = defaultdict(int)
    
    for index, row in df.iterrows():
        if row['Category'] in ["Professional Services", "Personal"]:
            description = row['Description']
            
            # Check if this description has already been categorized
            category = next((v for k, v in category_map.items() if k in description), None)
            
            if category is None:
                print(f"\nTransaction: {description}")
                print("Current category:", row['Category'])
                print("Choose a new category:")
                new_category = get_user_choice(unique_categories)
                df.at[index, 'Category'] = new_category
                
                prompt_count[description] += 1
                
                # If this description has been prompted more than twice, add it to category_map
                if prompt_count[description] > 1:
                    key = description.split()[0]  # Use the first word of the description as the key
                    category_map[key] = new_category
                    print(f"Memorized: {key} -> {new_category}")
            else:
                df.at[index, 'Category'] = category

    try:
        df.to_csv('finance-2024.csv', index=False)

	# Connect to a persistent database
	#conn = duckdb.connect('finance-2024.db')

	# Transaction Date,Description,Category,Type,Amount,Memo
	# Create table if it does not exist
	#conn.execute("CREATE TABLE IF NOT EXISTS transactions ('Transaction Date' DATE, Description VARCHAR, Category VARCHAR, Type VARCHAR, Amount DOUBLE, Memo VARCHAR)")

	# Insert data only if the combination of id and name does not exist
	#conn.execute(f"""
	#	INSERT INTO transactions ('Transaction Date',Description,Category,Type,Amount,Memo)
	#	SELECT {new_id}, '{new_name}'
	#	WHERE NOT EXISTS (
	#	    SELECT 1 FROM my_table WHERE id = {new_id} AND name = '{new_name}'
	#	)
	#""")	

        print("Processing complete. Output saved to finance-2024.csv")
    except PermissionError:
        print("Error: Permission denied when trying to save the file. Make sure you have write access to the current directory.")
    except Exception as e:
        print(f"An unexpected error occurred while saving the file: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    process_csv(input_file)
