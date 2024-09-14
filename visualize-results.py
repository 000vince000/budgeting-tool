import os
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import webbrowser
from db_operations import query_and_return_df, get_month_summary, execute_query
import math
import json

def print_divider(title):
    print("\n" + "=" * 40)
    print(title)
    print("=" * 40)

def create_plot(df):
    plt.figure(figsize=(15, 10))
    x = range(len(df))
    width = 0.6

    bars = plt.bar(x, df['specified_month_sum'], width, label='Specified Month Sum', color='skyblue', alpha=0.7)
    plt.scatter(x, df['p50_monthly_sum'], color='green', marker='s', s=50, label='P50')
    plt.scatter(x, df['p85_monthly_sum'], color='purple', marker='^', s=50, label='P85')

    plt.xlabel('Category')
    plt.ylabel('Amount')
    plt.title('Specified Month Sum with P50 and P85 Markers')
    plt.xticks(x, df['Category'], rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()

    for bar in bars:
        height = bar.get_height()
        rounded_height = math.ceil(height)
        plt.text(bar.get_x() + bar.get_width()/2., height, f'${rounded_height}', 
                 ha='center', va='bottom')

    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))

def calculate_net_income(df):
    income_categories = ['Salary', 'Rental income']
    income = df[df['Category'].isin(income_categories)]['specified_month_sum'].sum()
    expenses = df[~df['Category'].isin(income_categories)]['specified_month_sum'].sum()
    net_income = income - expenses
    
    print_divider("Income and Expense Summary")
    print(f"Total Income (Salary + Rental income): ${income:.2f}")
    print(f"Total Expenses: ${expenses:.2f}")
    print(f"Net Income: ${net_income:.2f}")

    return net_income

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

def main(year, month):
    """
    Main function to visualize and analyze financial data for a specified month.

    This function:
    1. Connects to the database and retrieves the month summary data.
    2. Prints a summary of the specified month's financial data.
    3. Calculates and displays the net income for the month.
    4. Display the month's goals and goal breakdown items if they exist.
    5. Display the goal progress as the breakdown item's amount minus the category total.
    6. Creates a bar plot comparing specified month sum with P50 and P85 markers for each category.
    7. Saves the plot as an image file and opens it in the default web browser.

    The function excludes income categories (Salary and Rental income) from the visualization
    to focus on expense categories.

    Args:
    year (int): The year for which to generate the report.
    month (int): The month (1-12) for which to generate the report.
    """
    db_name = 'budgeting-tool.db'

    conn = duckdb.connect(db_name)

    # Use the provided year and month instead of asking for user input
    df = get_month_summary(conn, year, month)
    
    # Get the month name from the dataframe
    month_name = df['Month'].iloc[0]
    
    # Update output filename to include month and year
    output_file = f'spending_comparison_{month_name}_{year}.png'
    
    print_divider(f"Month Summary - {month_name} {year}")
    print(df.drop(columns=['Month', 'Year']))  # Drop Month and Year columns from display

    net_income = calculate_net_income(df)

    excluded_categories = ['Salary', 'Rental income']
    df_filtered = df[~df['Category'].isin(excluded_categories)].sort_values('specified_month_sum', ascending=False)

    # Remove these lines:
    # goals = display_goals_and_breakdown_items(conn, year, month)
    # display_goal_progress(conn, year, month, df, goals)

    # Add this line instead:
    display_goal_progress(conn, year, month)
    
    create_plot(df_filtered)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    
    print_divider("Plot Generation")
    print(f"Plot saved as {output_file}")
    full_path = os.path.abspath(output_file)
    print(f"File exists: {os.path.exists(output_file)}")
    print(f"Full path: {full_path}")
    
    webbrowser.open(f'file://{full_path}')

    conn.close()

def display_goals_and_breakdown_items(conn, year, month):
    """
    Display the goals and their breakdown items for the specified month.

    Args:
    conn: Database connection object
    year (int): The year to display goals for
    month (int): The month to display goals for
    """
    print_divider("Goals and Breakdown Items")

    # Query to get goals for the specified month
    query = """
    SELECT category, description, amount
    FROM surplus_and_deficit_breakdown_items
    WHERE date = make_date(?, ?, 1)
    """
    
    result = query_and_return_df(conn, query, [year, month])
    
    if not result.empty:
        print(f"Goals for {year}-{month:02d}:")
        for _, item in result.iterrows():
            category = item['category'] if pd.notna(item['category']) else "Custom Goal"
            description = item['description'] if pd.notna(item['description']) else category
            print(f"  {category}: {description}: ${item['amount']:.2f}")
    else:
        print(f"No goals found for {year}-{month:02d}.")

    return result

def display_single_goal_progress(description, goal_amount, actual_amount):
    progress = goal_amount - actual_amount
    print(f"{description}:")
    print(f"  Goal: ${goal_amount:.2f}")
    print(f"  Actual: ${actual_amount:.2f}")
    print(f"  Progress: ${progress:.2f}")

def display_goal_progress(conn, year, month):
    """
    Display the goal progress based on active breakdowns for the specified month.

    Args:
    conn: Database connection object
    year (int): The year to display goal progress for
    month (int): The month to display goal progress for
    """
    print_divider("Goal Progress")

    # Step 1: Retrieve active breakdowns for the specified month
    active_breakdowns_query = """
    SELECT id, description
    FROM surplus_and_deficit_breakdowns
    WHERE effective_date <= make_date(?, ?, 1)
      AND (terminal_date IS NULL OR terminal_date >= make_date(?, ?, 1))
    """
    active_breakdowns = query_and_return_df(conn, active_breakdowns_query, [year, month, year, month])

    if active_breakdowns.empty:
        print(f"No active goals found for {year}-{month:02d}.")
        return

    # Step 2 & 3: Retrieve and sum up breakdown items for active breakdowns
    breakdown_items_query = """
    SELECT description, SUM(amount) as goal_amount
    FROM surplus_and_deficit_breakdown_items
    WHERE surplus_and_deficit_breakdown_id IN (
        SELECT id
        FROM surplus_and_deficit_breakdowns
        WHERE effective_date <= make_date(?, ?, 1)
          AND (terminal_date IS NULL OR terminal_date >= make_date(?, ?, 1))
    )
    GROUP BY description
    """
    goals = query_and_return_df(conn, breakdown_items_query, [year, month, year, month])

    # Retrieve actual spending for the month
    actual_spending_query = """
    SELECT Category, SUM(Amount) as actual_amount
    FROM consolidated_transactions
    WHERE strftime('%Y', "Transaction Date") = ?
      AND strftime('%m', "Transaction Date") = ?
    GROUP BY Category
    """
    actual_spending = query_and_return_df(conn, actual_spending_query, [str(year), str(month).zfill(2)])

    # Step 4: Calculate and display goal progress
    if not goals.empty:
        for _, goal in goals.iterrows():
            description = goal['description']
            goal_amount = goal['goal_amount']
            
            # Find the actual spending for this category in the month summary
            actual_amount = actual_spending[actual_spending['Category'] == description]['actual_amount'].values
            actual_amount = actual_amount[0] if len(actual_amount) > 0 else 0
            
            display_single_goal_progress(description, goal_amount, actual_amount)
            
    else:
        print(f"No goal items found for {year}-{month:02d}.")