import os
import sys
import shutil
import platform
import subprocess
import pandas as pd
import duckdb
import matplotlib.pyplot as plt
import webbrowser
from db_operations import query_and_return_df, get_month_summary, execute_query, get_active_breakdowns, get_breakdown_items, get_actual_spending, get_goals_and_breakdown_items, get_breakdown_items_by_date, get_subtotal_by_category_group_for_month, get_category_group_summary_with_percentiles
import math
from transactions import calculate_and_conditionally_insert_monthly_breakdowns
import json
import hashlib
from rich.console import Console
from rich.table import Table
from rich import box

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

def _income_color(value, p50, p85):
    if value == 0:
        return "dim"
    if p85 > 0 and value >= p85 * 1.25:
        return "bright_green"
    if p85 > 0 and value >= p85:
        return "green"
    if p85 > 0 and value >= p85 * 0.75:
        return "yellow"
    if p50 > 0 and value >= p50 * 0.5:
        return "orange1"
    return "red"

def _spending_color(value, p50, p85):
    if value == 0:
        return "dim"
    if p85 > 0 and value > p85 * 1.25:
        return "bright_red"
    if p85 > 0 and value > p85:
        return "red"
    if p85 > 0 and value > p85 * 0.75:
        return "orange1"
    if p50 > 0 and value > p50:
        return "yellow"
    if p50 > 0 and value > p50 * 0.5:
        return "green"
    return "bright_green"

def display_cli_spending_table(df, month_name, year):
    SIGNIFICANCE_THRESHOLD = 10  # hide categories where both this month and p85 are below this
    df = df[(df['specified_month_sum'] >= SIGNIFICANCE_THRESHOLD) | (df['p85_monthly_sum'] >= SIGNIFICANCE_THRESHOLD)].copy()

    if df.empty:
        print("No significant spending to display.")
        return

    max_val = df['specified_month_sum'].max()
    BAR_WIDTH = 20

    def make_bar(value, color):
        if max_val == 0:
            return " " * BAR_WIDTH
        filled = round((value / max_val) * BAR_WIDTH)
        return f"[{color}]{'█' * filled}[/{color}][dim]{'░' * (BAR_WIDTH - filled)}[/dim]"

    def budget_style(status):
        if not status:
            return ""
        if "Over" in status:
            return f"[red]{status}[/red]"
        if "Under" in status:
            return f"[green]{status}[/green]"
        return status

    console = Console()
    table = Table(
        title=f"Spending — {month_name} {year}",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
        pad_edge=True,
    )
    table.add_column("Category", style="bold", no_wrap=True)
    table.add_column("", no_wrap=True)  # bar
    table.add_column("This Month", justify="right")
    table.add_column("P50", justify="right", style="dim")
    table.add_column("P85", justify="right", style="dim")
    table.add_column("Budget Status", no_wrap=True)

    for _, row in df.iterrows():
        is_credit = bool(row.get('is_net_credit', False))
        if is_credit:
            color = "bright_green"
            amount_str = f"[{color}]+${row['specified_month_sum']:,.0f}[/{color}]"
        else:
            color = _spending_color(row['specified_month_sum'], row['p50_monthly_sum'], row['p85_monthly_sum'])
            amount_str = f"[{color}]${row['specified_month_sum']:,.0f}[/{color}]"
        table.add_row(
            row['category'],
            make_bar(row['specified_month_sum'], color),
            amount_str,
            f"${row['p50_monthly_sum']:,.0f}",
            f"${row['p85_monthly_sum']:,.0f}",
            budget_style(row['budget_status']),
        )

    console.print(table)

def _open_file(path):
    """Open a file with the OS default application.

    Handles WSL, where webbrowser/xdg-open can't find a default app for images
    and fail with a gio error. There we hand the file to Windows via explorer.exe
    (using a Windows-style path from wslpath), preferring wslview if installed.
    """
    try:
        if 'microsoft' in platform.uname().release.lower():  # WSL
            if shutil.which('wslview'):
                subprocess.run(['wslview', path], check=False)
            else:
                win = subprocess.run(['wslpath', '-w', path],
                                     capture_output=True, text=True).stdout.strip()
                # explorer.exe returns a non-zero code even on success, so don't check.
                subprocess.run(['explorer.exe', win or path], check=False)
        elif sys.platform == 'darwin':
            subprocess.run(['open', path], check=False)
        elif sys.platform.startswith('win'):
            os.startfile(path)  # noqa: only exists on Windows
        elif shutil.which('xdg-open'):
            subprocess.run(['xdg-open', path], check=False)
        else:
            webbrowser.open(f'file://{path}')
    except Exception as e:
        print(f"Couldn't auto-open the image ({e}). It's saved at: {path}")


GRAPHS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'graphs')


def open_graph(df, month_name, year):
    os.makedirs(GRAPHS_DIR, exist_ok=True)
    output_file = os.path.join(GRAPHS_DIR, f'spending_comparison_{month_name}_{year}.png')
    create_plot(df)
    temp_file = os.path.join(GRAPHS_DIR, 'temp_plot.png')
    plt.savefig(temp_file, dpi=300, bbox_inches='tight')
    new_hash = get_file_hash(temp_file)
    if os.path.exists(output_file):
        existing_hash = get_file_hash(output_file)
        if new_hash == existing_hash:
            print(f"Plot unchanged. Keeping existing file: {output_file}")
            os.remove(temp_file)
        else:
            os.replace(temp_file, output_file)
            print(f"Plot updated. Saved as: {output_file}")
    else:
        os.rename(temp_file, output_file)
        print(f"New plot saved as: {output_file}")
    full_path = os.path.abspath(output_file)
    _open_file(full_path)

# calculate net_income, per categories.category_group, as revenue - cost of revenue - discretionary_expenses - non_discretionary_expenses
def calculate_net_income(conn, year, month):
    GROUPS = ['Revenue', 'Cost of revenue', 'Discretionary', 'Non-discretionary', 'Misc']
    EXPENSE_LABEL = {
        'Revenue': 'Revenue',
        'Cost of revenue': '- Cost of Revenue',
        'Discretionary': '- Discretionary',
        'Non-discretionary': '- Non-Discretionary',
        'Misc': '- Misc',
    }

    df = get_category_group_summary_with_percentiles(conn, year, month)
    rows = {row['category_group']: row for _, row in df.iterrows()} if not df.empty else {}

    net_income = sum(rows[g]['subtotal'] for g in GROUPS if g in rows)

    console = Console()
    table = Table(
        title="Income and Expense Summary",
        box=box.SIMPLE_HEAVY,
        show_lines=False,
        pad_edge=True,
    )
    table.add_column("Group", style="bold", no_wrap=True)
    table.add_column("This Month", justify="right")
    table.add_column("P50", justify="right", style="dim")
    table.add_column("P85", justify="right", style="dim")

    for group in GROUPS:
        label = EXPENSE_LABEL[group]
        if group in rows:
            r = rows[group]
            subtotal = r['subtotal']
            p50 = r['p50']
            p85 = r['p85']
            if group == 'Revenue':
                color = _income_color(subtotal, p50, p85)
                table.add_row(
                    label,
                    f"[{color}]${subtotal:,.2f}[/{color}]",
                    f"${p50:,.2f}",
                    f"${p85:,.2f}",
                )
            else:
                if subtotal > 0:
                    color = "bright_green"
                    table.add_row(
                        label,
                        f"[{color}]+${subtotal:,.2f}[/{color}]",
                        f"-${p50:,.2f}",
                        f"-${p85:,.2f}",
                    )
                else:
                    color = _spending_color(abs(subtotal), p50, p85)
                    table.add_row(
                        label,
                        f"[{color}]-${abs(subtotal):,.2f}[/{color}]",
                        f"-${p50:,.2f}",
                        f"-${p85:,.2f}",
                    )
        else:
            table.add_row(label, "[dim]$0.00[/dim]", "—", "—")

    net_color = "green" if net_income >= 0 else "red"
    table.add_section()
    table.add_row(
        "[bold]Net Income[/bold]",
        f"[bold {net_color}]${net_income:,.2f}[/bold {net_color}]",
        "",
        "",
    )

    console.print(table)
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

    #check if breakdown items exist for the given month
    if get_breakdown_items_by_date(conn, year, month).empty:
        # ask user if they want to insert the breakdown items
        user_input = input(f"Insert breakdown items for {year}-{month:02d}? (y/n): ")
        if user_input.lower() == 'y':
            print(f"Inserting breakdown items for {year}-{month:02d}.")
            calculate_and_conditionally_insert_monthly_breakdowns(conn, int(active_breakdowns.iloc[0]['id']), f"{year}-{month:02d}-01")
    else:
        print(f"Breakdown items found for {year}-{month:02d}.")

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
    6. Displays the spending table (category, this-month spend, P50, P85, budget status).
    7. Optionally, on user confirmation, saves a bar-plot PNG and opens it in the browser.

    The function excludes income categories (Business revenue, Salary, Rental income) from the visualization
    to focus on expense categories.

    Args:
    year (int): The year for which to generate the report.
    month (int): The month (1-12) for which to generate the report.
    """
    db_name = 'budgeting-tool.db'
    conn = duckdb.connect(db_name)

    df = get_month_summary(conn, year, month)
    month_name = df['Month'].iloc[0]

    calculate_net_income(conn, year, month)

    df_filtered = df[df['category_group'] != 'Revenue'].sort_values('specified_month_sum', ascending=False)

    display_goal_progress(conn, year, month)
    display_cli_spending_table(df_filtered, month_name, year)

    if input("\nOpen PNG graph? (y/n): ").strip().lower() == 'y':
        open_graph(df_filtered, month_name, year)

    conn.close()