import duckdb
import os
import importlib.util
from datetime import datetime
from helpers import print_ascii_title, get_user_specified_date, print_divider, print_numbered_list, get_user_choice
from transactions import (dig_into_category, show_p95_expensive_nonrecurring,
                          review_extraordinary_spendings, set_budget,
                          add_adjustment_transaction, set_goals)

def run_visualize_script(year, month):
    script_path = os.path.join(os.path.dirname(__file__), 'visualize-results.py')
    spec = importlib.util.spec_from_file_location("visualize_module", script_path)
    visualize_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(visualize_module)
    visualize_module.main(year, month)

def main_menu(conn, year, month):
    menu_options = [
        "See spending profile",
        "Dig into a specific category",
        "See 95th percentile most expensive nonrecurring spendings",
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
        print_numbered_list(menu_options)
        print("=" * 50)  # Add a bottom border
        
        choice = get_user_choice("Enter your choice: ", range(1, len(menu_options) + 1))
        
        if choice == 1:
            run_visualize_script(year, month)
        elif choice == 2:
            dig_into_category(conn, year, month)
        elif choice == 3:
            show_p95_expensive_nonrecurring(conn, year, month)
        elif choice == 4:
            review_extraordinary_spendings(conn, year, month)
        elif choice == 5:
            set_budget(conn)
        elif choice == 6:
            add_adjustment_transaction(conn, year, month)
        elif choice == 7:
            set_goals(conn)
        elif choice == 8:
            return True  # Signal to change the analysis period
        elif choice == 9:
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
