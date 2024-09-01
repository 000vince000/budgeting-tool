import db_operations
import os
import importlib.util

# Get the directory of the current script
current_dir = os.path.dirname(os.path.abspath(__file__))

def main_menu():
    while True:
        print("\nMain Menu:")
        print("1. See latest month's spending profile")
        print("2. Set budget")
        print("3. Exit")
        
        choice = input("Enter your choice (1-3): ")
        
        if choice == '1':
            run_visualize_script()
        elif choice == '2':
            set_budget()
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

def set_budget():
    categories = db_operations.get_global_categories_from_db()
    
    print("\nSet your monthly budget for each category:")
    for category in categories:
        while True:
            try:
                amount = float(input(f"Budget for {category}: $"))
                print(f"Budget for {category} set to ${amount:.2f}")
                break
            except ValueError:
                print("Please enter a valid number.")

if __name__ == "__main__":
    main_menu()
