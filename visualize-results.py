import os
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import webbrowser
from db_operations import query_and_return_df
import math

def print_divider(title):
    print("\n" + "=" * 40)
    print(title)
    print("=" * 40)

def create_plot(df):
    plt.figure(figsize=(15, 10))
    x = range(len(df))
    width = 0.6

    bars = plt.bar(x, df['latest_month_sum'], width, label='Latest Month Sum', color='skyblue', alpha=0.7)
    #plt.scatter(x, df['avg_monthly_sum'], color='red', marker='o', s=50, label='Average')
    plt.scatter(x, df['p50_monthly_sum'], color='green', marker='s', s=50, label='P50')
    plt.scatter(x, df['p85_monthly_sum'], color='purple', marker='^', s=50, label='P85')

    plt.xlabel('Category')
    plt.ylabel('Amount')
    plt.title('Latest Month Sum with P50 and P85 Markers')
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
    income = df[df['Category'].isin(income_categories)]['latest_month_sum'].sum()
    expenses = df[~df['Category'].isin(income_categories)]['latest_month_sum'].sum()
    net_income = income - expenses
    
    print_divider("Income and Expense Summary")
    print(f"Total Income (Salary + Rental income): ${income:.2f}")
    print(f"Total Expenses: ${expenses:.2f}")
    print(f"Net Income: ${net_income:.2f}")

    return net_income

def main():
    db_name = 'budgeting-tool.db'
    output_file = 'spending_comparison.png'

    with open('latest-month-summary.sql', 'r') as file:
        query = file.read()

    conn = duckdb.connect(db_name)
    df = query_and_return_df(conn, query)
    
    print_divider("Latest Month Summary")
    print(df)

    net_income = calculate_net_income(df)

    excluded_categories = ['Salary', 'Rental income']
    df_filtered = df[~df['Category'].isin(excluded_categories)].sort_values('latest_month_sum', ascending=False)

    create_plot(df_filtered)

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    
    print_divider("Plot Generation")
    print(f"Plot saved as {output_file}")
    full_path = os.path.abspath(output_file)
    print(f"File exists: {os.path.exists(output_file)}")
    print(f"Full path: {full_path}")
    
    webbrowser.open(f'file://{full_path}')

if __name__ == "__main__":
    main()