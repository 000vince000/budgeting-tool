import pandas as pd
import sys
import os

def get_user_choice(description, unique_categories, user_choices):
    if description in user_choices:
        return user_choices[description]

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
                return unique_categories[choice - 1]
            elif choice == len(unique_categories) + 1:
                new_category = input("Enter the new category: ")
                user_choices[description] = new_category
                return new_category
            elif choice == len(unique_categories) + 2:
                user_choices[description] = "EXCLUDE"
                return "EXCLUDE"
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def process_csv(input_file):
    # Read only the necessary columns from the CSV file
    usecols = ['Date', 'Description', 'Type', 'Withdrawal', 'Deposit']
    df = pd.read_csv(input_file, usecols=usecols)
    
    print("Original data shape:")
    print(df.shape)
    print("\nFirst few rows of original data:")
    print(df.head())

    # Pre-filtering step: Remove rows where 'Description' contains 'CHASE CREDIT'
    df = df[~df['Description'].str.contains('CHASE CREDIT', case=False, na=False)]
    # Remove rows where 'Type' is 'TRANSFER'
    df = df[df['Type'] != 'TRANSFER']

    print("\nData shape after filtering:")
    print(df.shape)
    print("\nFirst few rows after filtering:")
    print(df.head())

    # Function to convert currency string to float
    def currency_to_float(x):
        if pd.isna(x):
            return 0.0
        return float(x.replace('$', '').replace(',', ''))

    # Convert 'Withdrawal' and 'Deposit' columns to numeric
    df['Withdrawal'] = df['Withdrawal'].apply(currency_to_float)
    df['Deposit'] = df['Deposit'].apply(currency_to_float)

    # Perform column operations
    df['Category'] = ''
    df['Amount'] = df['Deposit'] - df['Withdrawal']
    df['Memo'] = ''
    df['Transaction Date'] = df['Date']

    # Category mapping
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

    # Apply category mapping and prompt user for unmatched categories
    unique_categories = sorted(set(category_map.values()))
    rows_to_drop = []
    user_choices = {}

    for index, row in df.iterrows():
        description = row['Description']
        matched = False
        for key, value in category_map.items():
            if key.lower() in description.lower():
                df.at[index, 'Category'] = value
                matched = True
                break
        
        if not matched:
            user_choice = get_user_choice(description, unique_categories, user_choices)
            if user_choice == "EXCLUDE":
                rows_to_drop.append(index)
            else:
                df.at[index, 'Category'] = user_choice

    # Remove excluded rows
    df = df.drop(rows_to_drop)

    # Add 'Card' column with static value 'Schwab'
    df['Card'] = 'Schwab'

    print("\nFirst few rows after adding calculated fields and Card column:")
    print(df.head())

    # Reorder columns, putting 'Card' as the left-most column
    df = df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]
    
    # Generate output filename
    output_file = f"{os.path.splitext(input_file)[0]}-processed.csv"
    
    # Write to output file (overwrite if exists)
    df.to_csv(output_file, index=False)
    
    print(f"\nProcessed file saved as: {output_file}")
    print("\nFirst few rows of final data:")
    print(df.head())

    # Print some statistics about the categories
    for category in set(df['Category'].unique()):
        category_count = df['Category'].eq(category).sum()
        print(f"\nNumber of transactions marked as '{category}': {category_count}")
        print(f"Percentage of transactions marked as '{category}': {category_count / len(df) * 100:.2f}%")

    # Print memorized user choices
    print("\nMemorized user choices:")
    for desc, cat in user_choices.items():
        print(f"'{desc}' -> '{cat}'")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    process_csv(input_file)
