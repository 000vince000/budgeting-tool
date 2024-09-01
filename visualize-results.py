import os
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import webbrowser
from db_operations import query_and_return_df

def get_data(db_name, query):
    with duckdb.connect(db_name) as conn:
        return query_and_return_df(conn, query)

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
        plt.text(bar.get_x() + bar.get_width()/2., height, f'{height:.2f}', ha='center', va='bottom')

# Main execution
db_name = 'budgeting-tool.db'
output_file = 'spending_comparison.png'

with open('queries.sql', 'r') as file:
    query = file.read()

df = get_data(db_name, query)
print(df)

# Filter and sort data
excluded_categories = ['Salary', 'Rental income']
df_filtered = df[~df['Category'].isin(excluded_categories)].sort_values('latest_month_sum', ascending=False)

create_plot(df_filtered)

# Save and open the plot
plt.savefig(output_file, dpi=300, bbox_inches='tight')
print(f"Plot saved as {output_file}")

full_path = os.path.abspath(output_file)
print(f"File exists: {os.path.exists(output_file)}")
print(f"Full path: {full_path}")
webbrowser.open(f'file://{full_path}')