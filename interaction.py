import db_operations
import os
import importlib.util
import duckdb
import pandas as pd

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

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

def main_menu(conn):
    while True:
        print("\nMain Menu:")
        print("1. See latest month's spending profile")
        print("2. Dig into a specific category")
        print("3. Set budget")
        print("4. Exit")
        
        choice = input("Enter your choice (1-4): ")
        
        if choice == '1':
            run_visualize_script()
        elif choice == '2':
            dig_into_category(conn)
        elif choice == '3':
            set_budget(conn)
        elif choice == '4':
            break
        else:
            print("Invalid choice. Please try again.")

def run_visualize_script():
    visualize_script = os.path.join(current_dir, "visualize-results.py")
    
    if os.path.exists(visualize_script):
        spec = importlib.util.spec_from_file_location("visualize_results", visualize_script)
        visualize_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(visualize_module)
        
        if hasattr(visualize_module, 'main'):
            visualize_module.main()
        else:
            print("Error: main() function not found in visualize-results.py")
    else:
        print(f"Error: {visualize_script} does not exist")

def dig_into_category(conn):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        print(f"{len(categories) + 1}. Back to main menu")

        try:
            choice = int(input("\nChoose a category number to dig into: "))
            if choice == len(categories) + 1:
                break
            if 1 <= choice <= len(categories):
                selected_category = categories[choice - 1]
                query = """
                WITH latest_month AS (
                    SELECT DATE_TRUNC('month', MAX("Transaction Date")) AS month
                    FROM consolidated_transactions
                )
                SELECT id, "Transaction Date", Description, Amount
                FROM consolidated_transactions, latest_month
                WHERE Category = ?
                  AND DATE_TRUNC('month', "Transaction Date") = latest_month.month
                ORDER BY id
                """
                df = db_operations.query_and_return_df(conn, query, params=(selected_category,))
                if not df.empty:
                    print(f"\nTransactions for {selected_category} in the latest month:")
                    pd.set_option('display.max_rows', None)
                    pd.set_option('display.max_columns', None)
                    pd.set_option('display.width', None)
                    pd.set_option('display.max_colwidth', None)
                    print(df.to_string(index=False))
                    
                    while True:
                        recategorize = input("\nDo you want to recategorize any transaction? (y/n): ").lower()
                        if recategorize == 'n':
                            break
                        elif recategorize == 'y':
                            try:
                                transaction_id = int(input("Enter the ID of the transaction to recategorize: "))
                                if transaction_id in df['id'].values:
                                    print("\nAvailable categories:")
                                    for i, category in enumerate(categories, 1):
                                        print(f"{i}. {category}")
                                    new_category_index = int(input("Enter the number of the new category: "))
                                    if 1 <= new_category_index <= len(categories):
                                        new_category = categories[new_category_index - 1]
                                        db_operations.recategorize_transaction(conn, transaction_id, new_category, selected_category)
                                        print(f"Transaction {transaction_id} recategorized to {new_category}")
                                        # Refresh the dataframe
                                        df = db_operations.query_and_return_df(conn, query, params=(selected_category,))
                                        print("\nUpdated transactions:")
                                        print(df.to_string(index=False))
                                    else:
                                        print("Invalid category number.")
                                else:
                                    print("Invalid transaction ID.")
                            except ValueError:
                                print("Please enter a valid number.")
                        else:
                            print("Invalid input. Please enter 'y' or 'n'.")
                else:
                    print(f"No transactions found for {selected_category} in the latest month.")
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

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

if __name__ == "__main__":
    print_ascii_title()
    db_name = 'budgeting-tool.db'
    conn = duckdb.connect(db_name)
    try:
        main_menu(conn)
    finally:
        conn.close()
