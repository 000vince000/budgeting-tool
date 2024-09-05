import db_operations
import os
import importlib.util
import duckdb
import pandas as pd
from datetime import datetime

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

# Helper functions
def helper_print_categories(categories):
    for i, category in enumerate(categories, 1):
        print(f"{i}. {category}")
    print(f"{len(categories) + 1}. Back to main menu")

def helper_get_user_category_choice(categories):
    while True:
        try:
            choice = int(input("\nChoose a category number to dig into: "))
            if 1 <= choice <= len(categories) + 1:
                return choice
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def helper_print_transactions(df):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df.to_string(index=False))

def helper_get_recategorization_choice():
    while True:
        choice = input("\nDo you want to recategorize any transaction? (y/n): ").lower()
        if choice in ['y', 'n']:
            return choice
        print("Invalid input. Please enter 'y' or 'n'.")

def helper_get_transaction_id(df):
    while True:
        try:
            transaction_id = int(input("Enter the ID of the transaction to recategorize: "))
            if transaction_id in df['id'].values:
                return transaction_id
            print("Invalid transaction ID.")
        except ValueError:
            print("Please enter a valid number.")

def helper_get_new_category(categories):
    print("\nAvailable categories:")
    for i, category in enumerate(categories, 1):
        print(f"{i}. {category}")
    print(f"{len(categories) + 1}. Exclude (set category to NULL)")
    
    while True:
        try:
            choice = int(input("Enter the number of the new category or Exclude option: "))
            if 1 <= choice <= len(categories) + 1:
                return choice
            print("Invalid category number.")
        except ValueError:
            print("Please enter a valid number.")

def helper_get_transaction_date(year, month):
    while True:
        # ask user for date, but with the year and month already specified in the prompt which the user can change  
        date_str = input(f"Enter transaction date (YYYY-MM-DD, default is {year}-{month}): ")
        if date_str == "":
            return datetime(year, month, 1).date()
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid date format. Please use YYYY-MM-DD.")

def helper_get_transaction_amount():
    while True:
        amount_str = input("Enter amount (negative for expense, positive for income): ")
        try:
            return float(amount_str)
        except ValueError:
            print("Invalid amount. Please enter a number.")

def helper_get_transaction_category(conn):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    print("\nCategories:")
    for i, category in enumerate(categories, 1):
        print(f"{i}. {category}")
    print(f"{len(categories) + 1}. Other")
    
    while True:
        try:
            category_choice = int(input("Choose a category number: "))
            if 1 <= category_choice <= len(categories):
                return categories[category_choice - 1]
            elif category_choice == len(categories) + 1:
                return input("Enter custom category: ")
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

# Main functions
def print_ascii_title():
    ascii_art = r"""
 ____            _            _   _                _____           _ 
|  _ \          | |          | | (_)              |_   _|         | |
| |_) |_   _  __| | __ _  ___| |_ _ _ __   __ _     | |  ___   ___| |
|  _ <| | | |/ _` |/ _` |/ _ \ __| | '_ \ / _` |    | | / _ \ / _ \ |
| |_) | |_| | (_| | (_| |  __/ |_| | | | | (_| |    | || (_) | (_)| |
|____/ \__,_|\__,_|\__, |\___|\__|_|_| |_|\__, |    \_/ \___/ \___|_|
                    __/ |                  __/ |                     
                   |___/                  |___/                      
 _           _____ ___ ____      
| |_ ___    |  ___|_ _|  _ \  ___ 
| __/ _ \   | |_   | || |_) |/ _ \
| || (_) |  |  _|  | ||  _ <|  __/
 \__\___/   |_|   |___|_| \_\____|
                                  
           By Vince Chen
"""
    print(ascii_art)

def get_user_specified_date():
    while True:
        year = input("Enter the year (YYYY): ")
        month = input("Enter the month (1-12): ")
        try:
            year = int(year)
            month = int(month)
            if 1 <= month <= 12 and 1900 <= year <= 9999:
                return year, month
            else:
                print("Invalid year or month. Please try again.")
        except ValueError:
            print("Invalid input. Please enter numbers only.")

def print_divider(title):
    print("\n" + "=" * 50)
    print(title.center(50))
    print("=" * 50 + "\n")

def main_menu(conn, year, month):
    while True:
        print("\nMain Menu:")
        print(f"Current analysis period: {datetime(year, month, 1).strftime('%B %Y')}")
        print("1. See spending profile")
        print("2. Dig into a specific category")
        print("3. See 95th percentile most expensive nonrecurring spendings")
        print("4. Set budget")
        print("5. Add an adjustment transaction")
        print("6. Change analysis period")
        print("7. Exit")
        
        choice = input("Enter your choice (1-7): ")
        
        if choice == '1':
            run_visualize_script(year, month)
        elif choice == '2':
            dig_into_category(conn, year, month)
        elif choice == '3':
            df = db_operations.show_p95_expensive_nonrecurring_for_latest_month(conn, year, month)
            if df is None:
                print("No non-recurring expenses found.")
            else:
                print("\n95th percentile most expensive non-recurring spendings:")
                print(df.to_string(index=False))
                total = df['Amount'].sum()
                print(f"Total ........................... ${total:.2f}")
        elif choice == '4':
            set_budget(conn)
        elif choice == '5':
            add_adjustment_transaction(conn, year, month)
        elif choice == '6':
            return True  # Signal to change the analysis period
        elif choice == '7':
            return False  # Signal to exit the program
        else:
            print("Invalid choice. Please try again.")

def run_visualize_script(year, month):
    script_path = os.path.join(os.path.dirname(__file__), 'visualize-results.py')
    spec = importlib.util.spec_from_file_location("visualize_module", script_path)
    visualize_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(visualize_module)
    visualize_module.main(year, month)

def dig_into_category(conn, year, month):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        helper_print_categories(categories)

        choice = helper_get_user_category_choice(categories)
        if choice == len(categories) + 1:
            break

        selected_category = categories[choice - 1]
        df = db_operations.fetch_transactions(conn, selected_category, year, month)

        if df.empty:
            print(f"No transactions found for {selected_category} in {datetime(year, month, 1).strftime('%B %Y')}.")
            continue

        print(f"\nTransactions for {selected_category} in {datetime(year, month, 1).strftime('%B %Y')}:")
        helper_print_transactions(df)
        
        while helper_get_recategorization_choice() == 'y':
            transaction_id = helper_get_transaction_id(df)
            new_category_index = helper_get_new_category(categories)
            
            if new_category_index <= len(categories):
                new_category = categories[new_category_index - 1]
            else:
                new_category = None

            db_operations.recategorize_transaction(conn, transaction_id, new_category, selected_category)
            print(f"Transaction {transaction_id} recategorized to {new_category or 'NULL (Excluded)'}")
            
            # Refresh the dataframe
            df = db_operations.fetch_transactions(conn, selected_category, year, month)
            print("\nUpdated transactions:")
            helper_print_transactions(df)

def set_budget(conn):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        print(f"{len(categories) + 1}. Back to main menu")

        try:
            choice = int(input("\nChoose a category number to set budget: "))
            if choice == len(categories) + 1:
                break
            if 1 <= choice <= len(categories):
                selected_category = categories[choice - 1]
                while True:
                    try:
                        amount = float(input(f"Enter budget for {selected_category}: $"))
                        db_operations.insert_category_budget(conn, selected_category, int(amount))
                        print(f"Budget for {selected_category} set to ${amount:.2f}")
                        query = "SELECT * FROM current_budgets"
                        df = db_operations.query_and_return_df(conn, query)
                        print(df)
                        break
                    except ValueError:
                        print("Please enter a valid number.")
                    except Exception as e:
                        print(f"An error occurred: {str(e)}")
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def add_adjustment_transaction(conn, year, month):
    print("\nAdding an adjustment transaction:")
    
    transaction_date = helper_get_transaction_date(year, month)
    description = input("Enter transaction description: ")
    amount = helper_get_transaction_amount()
    category = helper_get_transaction_category(conn)
    
    try:
        db_operations.insert_adjustment_transaction(conn, transaction_date, description, amount, category)
        print("Adjustment transaction added successfully.")
    except Exception as e:
        print(f"Error adding transaction: {str(e)}")

if __name__ == "__main__":
    print_ascii_title()
    db_name = 'budgeting-tool.db'
    conn = duckdb.connect(db_name)

    while True:
        year, month = get_user_specified_date()
        change_period = main_menu(conn, year, month)
        if not change_period:
            break

    conn.close()
    print("Thank you for using the budgeting tool. Goodbye!")
