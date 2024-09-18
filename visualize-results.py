import os
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import webbrowser
from db_operations import query_and_return_df, get_month_summary, execute_query, get_active_breakdowns, get_breakdown_items, get_actual_spending, get_goals_and_breakdown_items
import math
import json
import hashlib  # Add this import

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
    plt.xticks(x, df['category'], rotation=45, ha='right')  # Changed 'Category' to 'category'
    plt.legend()
    plt.tight_layout()

    for bar in bars:
        height = bar.get_height()
        rounded_height = math.ceil(height)
        plt.text(bar.get_x() + bar.get_width()/2., height, f'${rounded_height}', 
                 ha='center', va='bottom')

    plt.gca().yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'${int(x):,}'))

# calculate net_income, per categories.category_group, as revenue - cost of revenue - discretionary_expenses - non_discretionary_expenses
def calculate_net_income(df):
    revenue = df[df['category_group'] == 'Revenue']['specified_month_sum'].sum()
    cost_of_revenue = df[df['category_group'] == 'Cost of revenue']['specified_month_sum'].sum()
    discretionary_expenses = df[df['category_group'] == 'Discretionary']['specified_month_sum'].sum()
    non_discretionary_expenses = df[df['category_group'] == 'Non-discretionary']['specified_month_sum'].sum()

    net_income = revenue - cost_of_revenue - discretionary_expenses - non_discretionary_expenses
    
    print_divider("Income and Expense Summary")
    print(f"Revenue:                     ${revenue:,.2f}")
    print(f"- Cost of Revenue:            ${cost_of_revenue:,.2f}")
    print(f"- Discretionary Expenses:     ${discretionary_expenses:,.2f}")
    print(f"- Non-Discretionary Expenses: ${non_discretionary_expenses:,.2f}")
    print("----------------------------------------")
    print(f"Net Income:                   ${net_income:,.2f}")

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

def display_goals_and_breakdown_items(conn, year, month):
    """
    Display the goals and their breakdown items for the specified month.

    Args:
    conn: Database connection object
    year (int): The year to display goals for
    month (int): The month to display goals for
    """
    print_divider("Goals and Breakdown Items")

    result = get_goals_and_breakdown_items(conn, year, month)
    
    if not result.empty:
        print(f"Goals for {year}-{month:02d}:")
        for _, item in result.iterrows():
            category = item['category'] if pd.notna(item['category']) else "Custom Goal"
            description = item['description'] if pd.notna(item['description']) else category
            print(f"  {category}: {description}: ${item['amount']:.2f}")
    else:
        print(f"No goals found for {year}-{month:02d}.")

    return result

def display_single_goal_progress(description, accumulation_amount, reduction):
    progress = accumulation_amount + reduction
    print(f"{description}:")
    print(f"  Gross Accumulation: ${accumulation_amount:.2f}")
    print(f"  Reduction: ${reduction:.2f}")
    print(f"  Net Accumulation: ${progress:.2f}")

def display_goal_progress(conn, year, month):
    print_divider("Goal Progress")

    active_breakdowns = get_active_breakdowns(conn, year, month)

    if active_breakdowns.empty:
        print(f"No active goals found for {year}-{month:02d}.")
        return

    accumulations = get_breakdown_items(conn, year, month)
    actual_spending = get_actual_spending(conn, year, month)

    if not accumulations.empty:
        # First, display Savings and Investment
        for category in ['Savings', 'Investment']:
            if category in accumulations['description'].values:
                accumulation = accumulations[accumulations['description'] == category]['accumulation'].values[0]
                latest_amount = accumulations[accumulations['description'] == category]['latest_amount'].values[0]
                print(f"{category}: ${accumulation:,.2f}")

                # Calculate 10-year projection with monthly accumulation
                cagr = 0.03 if category == 'Savings' else 0.08  # Reverted back to 0.03 and 0.08
                total_months = 10 * 12
                projection = 0
                for month in range(total_months):
                    projection += latest_amount
                    projection *= (1 + cagr / 12)  # Apply monthly growth rate

                print(f"  10-year projection (with {cagr*100:.1f}% CAGR and monthly ${latest_amount:,.2f} contribution): ${projection:,.2f}")

        # Then display other categories
        for _, accumulation in accumulations.iterrows():
            description = accumulation['description']
            if description not in ['Savings', 'Investment']:
                accumulation_amount = accumulation['accumulation']
                
                reduction = actual_spending[actual_spending['Category'] == description]['actual_amount'].values
                reduction = reduction[0] if len(reduction) > 0 else 0
                
                display_single_goal_progress(description, accumulation_amount, reduction)
    else:
        print(f"No goal items found for {year}-{month:02d}.")

def get_file_hash(filename):
    """Calculate the SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(filename, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

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
    output_file = f'spending_comparison_{month_name}_{year}.png'
    
    # print_divider(f"Month Summary - {month_name} {year}")
    # print(df.drop(columns=['Month', 'Year']))  # Drop Month and Year columns from display
 
    calculate_net_income(df)

    df_filtered = df[df['category_group'] != 'Revenue'].sort_values('specified_month_sum', ascending=False)

    display_goal_progress(conn, year, month)
    create_plot(df_filtered)

    # Save the plot to a temporary file
    temp_file = 'temp_plot.png'
    plt.savefig(temp_file, dpi=300, bbox_inches='tight')
    
    # Calculate hash of the new plot
    new_hash = get_file_hash(temp_file)

    # Check if the output file already exists
    if os.path.exists(output_file):
        existing_hash = get_file_hash(output_file)
        if new_hash == existing_hash:
            print(f"Plot unchanged. Keeping existing file: {output_file}")
            os.remove(temp_file)  # Remove the temporary file
        else:
            os.replace(temp_file, output_file)
            print(f"Plot updated. Saved as: {output_file}")
    else:
        os.rename(temp_file, output_file)
        print(f"New plot saved as: {output_file}")

    full_path = os.path.abspath(output_file)
    webbrowser.open(f'file://{full_path}')

    conn.close()