import db_operations
import os
import importlib.util
import duckdb
import pandas as pd
import json
from decimal import Decimal
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# Helper functions
def print_numbered_list(items, start=1):
    for i, item in enumerate(items, start):
        print(f"{i}. {item}")

def get_user_choice(prompt, valid_range):
    while True:
        try:
            choice = int(input(prompt))
            if choice in valid_range:
                return choice
            print("Invalid choice. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

def get_user_input(prompt, input_type=str, validation_func=None):
    while True:
        try:
            user_input = input_type(input(prompt))
            if validation_func is None or validation_func(user_input):
                return user_input
            print("Invalid input. Please try again.")
        except ValueError:
            print(f"Please enter a valid {input_type.__name__}.")

def print_dataframe(df):
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    print(df.to_string(index=False))

def print_divider(title):
    print("\n" + "=" * 50)
    print(title.center(50))
    print("=" * 50 + "\n")

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
    year = get_user_input("Enter the year (YYYY): ", int, lambda x: 1900 <= x <= 9999)
    month = get_user_input("Enter the month (1-12): ", int, lambda x: 1 <= x <= 12)
    return year, month

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
        print_numbered_list(categories + ["Back to main menu"])

        choice = get_user_choice("\nChoose a category number to dig into: ", range(1, len(categories) + 2))
        if choice == len(categories) + 1:
            break

        selected_category = categories[choice - 1]
        df = db_operations.fetch_transactions(conn, selected_category, year, month)

        if df.empty:
            print(f"No transactions found for {selected_category} in {datetime(year, month, 1).strftime('%B %Y')}.")
            continue

        print(f"\nTransactions for {selected_category} in {datetime(year, month, 1).strftime('%B %Y')}:")
        print_dataframe(df)
        
        while get_user_input("\nDo you want to recategorize any transaction? (y/n): ", str, lambda x: x.lower() in ['y', 'n']).lower() == 'y':
            recategorize_transaction(conn, df, categories, selected_category)
            df = db_operations.fetch_transactions(conn, selected_category, year, month)
            print("\nUpdated transactions:")
            print_dataframe(df)

def recategorize_transaction(conn, df, categories, selected_category):
    transaction_id = get_user_input("Enter the ID of the transaction to recategorize: ", int, lambda x: x in df['id'].values)
    new_category_index = get_user_choice("\nEnter the number of the new category or Exclude option: ", range(1, len(categories) + 2))
    
    new_category = categories[new_category_index - 1] if new_category_index <= len(categories) else None
    transaction = df[df['id'] == transaction_id].iloc[0]
    vendor = transaction['Description']

    if get_user_input(f"Do you want to apply this categorization to all transactions from '{vendor}'? (y/n): ", str, lambda x: x.lower() in ['y', 'n']).lower() == 'y':
        recategorize_all_vendor_transactions(conn, vendor, new_category)
    else:
        db_operations.recategorize_transaction(conn, transaction_id, new_category, selected_category)
        print(f"Transaction {transaction_id} recategorized to {new_category or 'NULL (Excluded)'}")

def recategorize_all_vendor_transactions(conn, vendor, new_category):
    try:
        conn.execute("BEGIN TRANSACTION")
        all_transactions = db_operations.get_transactions_by_vendor(conn, vendor)
        transaction_ids = all_transactions['id'].tolist()
        db_operations.recategorize_transactions(conn, transaction_ids, new_category)
        
        existing_mapping = db_operations.get_vendor_category_mapping(conn, vendor)
        if existing_mapping != new_category:
            if new_category is not None:
                db_operations.insert_vendor_category_mapping(conn, vendor, new_category)
                print(f"Vendor-category mapping added: '{vendor}' -> '{new_category}'")
            else:
                db_operations.delete_vendor_category_mapping(conn, vendor)
                print(f"Vendor-category mapping removed for '{vendor}'")
        
        conn.commit()
        print(f"All transactions from '{vendor}' have been recategorized to '{new_category or 'NULL (Excluded)'}'")
    except Exception as e:
        conn.rollback()
        print(f"An error occurred. All operations have been rolled back. Error: {str(e)}")

def set_budget(conn):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        print_numbered_list(categories + ["Back to main menu"])

        choice = get_user_choice("\nChoose a category number to set budget: ", range(1, len(categories) + 2))
        if choice == len(categories) + 1:
            break

        selected_category = categories[choice - 1]
        amount = get_user_input(f"Enter budget for {selected_category}: $", float)
        
        try:
            db_operations.insert_category_budget(conn, selected_category, int(amount))
            print(f"Budget for {selected_category} set to ${amount:.2f}")
            query = "SELECT * FROM current_budgets"
            df = db_operations.query_and_return_df(conn, query)
            print(df)
        except Exception as e:
            print(f"An error occurred: {str(e)}")

def add_adjustment_transaction(conn, year, month):
    print("\nAdding an adjustment transaction:")
    
    transaction_date = datetime(year, month, 1).date()
    description = input("Enter transaction description: ")
    amount = get_user_input("Enter amount (negative for expense, positive for income): ", float)
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    print("\nCategories:")
    print_numbered_list(categories + ["Other"])
    
    category_choice = get_user_choice("Choose a category number: ", range(1, len(categories) + 2))
    if category_choice <= len(categories):
        category = categories[category_choice - 1]
    else:
        category = input("Enter custom category: ")
    
    try:
        db_operations.insert_adjustment_transaction(conn, transaction_date, description, amount, category)
        print("Adjustment transaction added successfully.")
    except Exception as e:
        print(f"Error adding transaction: {str(e)}")

def set_goals(conn):
    print_divider("Setting a New Goal")
    description = input("Enter a description for this goal: ")
    breakdown = get_goal_breakdown(conn)
    if not breakdown:
        print("No goals set. Exiting goal setting.")
        return
    effective_date = get_user_input("Enter the effective date (YYYY-MM-DD): ", str, validate_date)
    
    try:
        with conn.cursor() as cursor:
            breakdown_id = insert_goal_breakdown(cursor, description, breakdown, effective_date)
            calculate_and_conditionally_insert_monthly_breakdowns(cursor, breakdown_id, breakdown, effective_date)
        conn.commit()
        print("Goal added successfully and monthly breakdowns calculated.")
    except Exception as e:
        conn.rollback()
        print(f"Error adding goal or calculating monthly breakdowns: {str(e)}")

def get_goal_breakdown(conn):
    breakdown = {}
    remaining_percentage = Decimal('100')
    categories = ['Investment', 'Savings'] + sorted(db_operations.get_global_categories_from_db(conn))

    # Ask for Investment and Savings first
    for category in ['Investment', 'Savings']:
        percentage = get_user_input(f"Enter percentage for {category} (0-{remaining_percentage}%): ", 
                                    Decimal, lambda x: 0 <= x <= remaining_percentage)
        if percentage > 0:
            breakdown[category] = percentage / 100
            remaining_percentage -= percentage
        categories.remove(category)

    while remaining_percentage > 0:
        print("\nAvailable categories:")
        print_numbered_list(categories)
        print(f"{len(categories) + 1}. Enter a new description")
        print(f"{len(categories) + 2}. Finish setting goals")

        choice = get_user_choice("\nChoose a category number or action: ", range(1, len(categories) + 3))
        
        if choice == len(categories) + 2:
            break
        elif choice == len(categories) + 1:
            description = input("Enter a new description: ")
            percentage = get_user_input(f"Enter percentage for this description (0-{remaining_percentage}%): ", 
                                        Decimal, lambda x: 0 <= x <= remaining_percentage)
            if percentage > 0:
                breakdown[description] = percentage / 100
                remaining_percentage -= percentage
        else:
            selected_category = categories[choice - 1]
            percentage = get_user_input(f"Enter percentage for {selected_category} (0-{remaining_percentage}%): ", 
                                        Decimal, lambda x: 0 <= x <= remaining_percentage)
            if percentage > 0:
                breakdown[selected_category] = percentage / 100
                remaining_percentage -= percentage
                categories.remove(selected_category)

        print(f"\nRemaining percentage: {remaining_percentage}%")

    if remaining_percentage > 0:
        print(f"Warning: {remaining_percentage}% of the budget was not allocated.")

    return breakdown

def insert_goal_breakdown(cursor, description, breakdown, effective_date):
    breakdown_json = json.dumps({k: str(v) for k, v in breakdown.items()})
    return db_operations.insert_surplus_deficit_breakdown(
        cursor, description, breakdown_json, effective_date
    )

def insert_single_breakdown_item(cursor, breakdown_id, category_or_description, percentage, net_income, current_date, valid_categories):
    amount = net_income * Decimal(percentage)
    if category_or_description in valid_categories:
        category = description = category_or_description
    else:
        category, description = None, category_or_description
    db_operations.insert_surplus_deficit_breakdown_item(
        cursor, breakdown_id, category, description, amount, current_date
    )

def calculate_and_conditionally_insert_monthly_breakdowns(cursor, breakdown_id, breakdown, effective_date):
    """
    Calculate and insert monthly breakdowns for a given goal.

    This function:
    1. Determines the date range from the effective date to the latest transaction date.
    2. For each month in this range:
       a. Calculates the net income for that month.
       b. Applies the goal breakdown percentages to the net income.
       c. Inserts breakdown items for each category when current_date is in a month that has already ended.

    Args:
    cursor: Database cursor for executing queries.
    breakdown_id: ID of the goal breakdown.
    breakdown: Dictionary of category/description percentages for the goal.
    effective_date: Start date for calculating breakdowns.
    """
    valid_categories = set(db_operations.get_global_categories_from_db(cursor))
    latest_transaction_date = db_operations.get_latest_transaction_date(cursor)
    
    current_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
    end_date = latest_transaction_date.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
    today = date.today()

    while current_date <= end_date:
        # Only process months that have ended
        if current_date.replace(day=1) + relativedelta(months=1) <= today:
            net_income = db_operations.get_net_income_for_month(cursor, current_date.year, current_date.month)
            for category_or_description, percentage in breakdown.items():
                insert_single_breakdown_item(cursor, breakdown_id, category_or_description, percentage, net_income, current_date, valid_categories)

        current_date += relativedelta(months=1)

def validate_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return False

def main_menu(conn, year, month):
    menu_options = [
        "See spending profile",
        "Dig into a specific category",
        "See 95th percentile most expensive nonrecurring spendings",
        "Set budget",
        "Add an adjustment transaction",
        "Set goals (Surplus/Deficit Breakdown)",
        "Change analysis period",
        "Exit"
    ]

    while True:
        print("\nMain Menu:")
        print(f"Current analysis period: {datetime(year, month, 1).strftime('%B %Y')}")
        print_numbered_list(menu_options)
        
        choice = get_user_choice("Enter your choice: ", range(1, len(menu_options) + 1))
        
        if choice == 1:
            run_visualize_script(year, month)
        elif choice == 2:
            dig_into_category(conn, year, month)
        elif choice == 3:
            show_p95_expensive_nonrecurring(conn, year, month)
        elif choice == 4:
            set_budget(conn)
        elif choice == 5:
            add_adjustment_transaction(conn, year, month)
        elif choice == 6:
            set_goals(conn)
        elif choice == 7:
            return True  # Signal to change the analysis period
        elif choice == 8:
            return False  # Signal to exit the program

def show_p95_expensive_nonrecurring(conn, year, month):
    df = db_operations.show_p95_expensive_nonrecurring_for_latest_month(conn, year, month)
    if df is None:
        print("No non-recurring expenses found.")
    else:
        print("\n95th percentile most expensive non-recurring spendings:")
        print_dataframe(df)
        total = df['Amount'].sum()
        print(f"Total ........................... ${total:.2f}")

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
