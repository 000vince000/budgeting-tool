import duckdb
import os
import importlib.util
from datetime import datetime
from helpers import print_ascii_title, get_user_specified_date, print_divider, print_numbered_list, get_user_choice
from transactions import (dig_into_category, show_biggest_oneoff_expenses,
                          review_extraordinary_spendings, set_budget,
                          add_adjustment_transaction, set_goals,
                          show_flagged_transactions, dig_into_category_group, search_transactions_by_keyword)

def run_visualize_script(year, month):
    script_path = os.path.join(os.path.dirname(__file__), 'visualize-results.py')
    spec = importlib.util.spec_from_file_location("visualize_module", script_path)
    visualize_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(visualize_module)
    visualize_module.main(year, month)

def main_menu(conn, year, month):
    menu_options = [
        "See spending profile",
        "See flagged transactions",
        "Search transactions by keyword",
        "Dig into a specific category",
        "Dig into a specific category group",
        "See biggest one-off expenses",
        "Review extraordinary spendings",
        "Set budget",
        "Add an adjustment transaction",
        "Set goals (Surplus/Deficit Breakdown)",
        "Change analysis period",
        "Exit"
    ]

    while True:
        print_divider("Main Menu")
        print(f"Current analysis period: {datetime(year, month, 1).strftime('%B %Y')}")
        print_numbered_list(menu_options[:-1])  # Print all options except the last one with numbers
        print(f"x. {menu_options[-1]}")  # Print the exit option with 'x'
        print("=" * 50)  # Add a bottom border
        
        choice = get_user_choice("Enter your choice: ", list(range(1, len(menu_options))) + ['x'])
        
        if choice == 1:
            run_visualize_script(year, month)
        elif choice == 2:
            show_flagged_transactions(conn)  # New function call
        elif choice == 3:
            search_transactions_by_keyword(conn, year, month)
        elif choice == 4:
            dig_into_category(conn, year, month)
        elif choice == 5:
            dig_into_category_group(conn, year, month)
        elif choice == 6:
            show_biggest_oneoff_expenses(conn, year, month)
        elif choice == 7:
            review_extraordinary_spendings(conn, year, month)
        elif choice == 8:
            set_budget(conn)
        elif choice == 9:
            add_adjustment_transaction(conn, year, month)
        elif choice == 10:
            set_goals(conn)
        elif choice == 11:
            return True  # Signal to change the analysis period
        elif choice == 'x':
            return False  # Signal to exit the program

def main():
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

if __name__ == "__main__":
    main()
