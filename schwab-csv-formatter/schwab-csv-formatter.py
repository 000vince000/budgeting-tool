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
    df['Blank1'] = ''
    df['Blank2'] = ''
    df['Combined'] = df['Deposit'] - df['Withdrawal']

    # Calculated field: Mark 'Transportation' in 'Blank2' column
    transportation_mask = df['Description'].str.contains('E-ZPASS|NYC FINANCE PARKING', case=False, na=False)
    df.loc[transportation_mask, 'Blank2'] = 'Transportation'

    # Add 'Card' column with static value 'Schwab'
    df['Card'] = 'Schwab'

    print("\nFirst few rows after adding calculated fields and Card column:")
    print(df.head())

    # Reorder columns, putting 'Card' as the left-most column
    df = df[['Card', 'Date', 'Blank1', 'Description', 'Blank2', 'Type', 'Combined']]
    
    # Generate output filename
    output_file = f"{os.path.splitext(input_file)[0]}-processed.csv"
    
    # Write to output file (overwrite if exists)
    df.to_csv(output_file, index=False)
    
    print(f"\nProcessed file saved as: {output_file}")
    print("\nFirst few rows of final data:")
    print(df.head())

    # Print some statistics about the 'Transportation' category
    transport_count = df['Blank2'].eq('Transportation').sum()
    print(f"\nNumber of transactions marked as 'Transportation': {transport_count}")
    print(f"Percentage of transactions marked as 'Transportation': {transport_count / len(df) * 100:.2f}%")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    process_csv(input_file)
