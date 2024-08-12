import pandas as pd
import sys
import os

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
        'GRUBHUB HOLDING': 'Salary',
        'NYCSHININGSMILES NYCSHINING': 'Kids',
        'WEB PMTS': 'Monthly property expense',
        'MORTGAGE': 'Monthly mortgage expense',
        'NAJERA-ESTEBAN': 'Kids'
    }

    # Apply category mapping
    for key, value in category_map.items():
        mask = df['Description'].str.contains(key, case=False, na=False)
        df.loc[mask, 'Category'] = value

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
    for category in set(category_map.values()):
        category_count = df['Category'].eq(category).sum()
        print(f"\nNumber of transactions marked as '{category}': {category_count}")
        print(f"Percentage of transactions marked as '{category}': {category_count / len(df) * 100:.2f}%")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    process_csv(input_file)
