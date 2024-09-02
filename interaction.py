import db_operations
import os
import importlib.util
import duckdb
# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

def main_menu(conn):
    while True:
        print("\nMain Menu:")
        print("1. See latest month's spending profile")
        print("2. Set budget")
        print("3. Exit")
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            run_visualize_script()
        elif choice == '2':
            set_budget(conn)
        elif choice == '3':
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
    db_name = 'budgeting-tool.db'
    conn = duckdb.connect(db_name)
    try:
        main_menu(conn)
    finally:
        conn.close()
