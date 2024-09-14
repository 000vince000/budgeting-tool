import db_operations
from helpers import print_divider, print_dataframe, get_user_input, get_user_choice
from collections import defaultdict

def dig_into_category(conn, year, month):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        print(f"{len(categories) + 1}. Back to main menu")

        choice = get_user_choice("\nChoose a category number to dig into: ", range(1, len(categories) + 2))
        if choice == len(categories) + 1:
            break

        selected_category = categories[choice - 1]
        df = db_operations.fetch_transactions(conn, selected_category, year, month)

        if df.empty:
            print(f"No transactions found for {selected_category} in {year}-{month:02d}.")
            continue

        print(f"\nTransactions for {selected_category} in {year}-{month:02d}:")
        print_dataframe(df)
        
        while get_user_input("\nDo you want to recategorize any transaction? (y/n): ", str, lambda x: x.lower() in ['y', 'n']).lower() == 'y':
            recategorize_transaction(conn, df, categories, selected_category)
            df = db_operations.fetch_transactions(conn, selected_category, year, month)
            print("\nUpdated transactions:")
            print_dataframe(df)

def show_p95_expensive_nonrecurring(conn, year, month):
    print_divider("95th Percentile Most Expensive Non-recurring Spendings")
    df = db_operations.show_p95_expensive_nonrecurring_for_latest_month(conn, year, month)
    if df is None or df.empty:
        print("No non-recurring expenses found.")
    else:
        total = 0
        for _, row in df.iterrows():
            date = row['Transaction Date'].strftime('%Y-%m-%d')
            amount = abs(row['Amount'])
            print(f"[{row['Category']:<20}] {date}\t{row['Description']:<40}\t${amount:>10.2f}")
            total += amount
        print(f"\nTotal 95th percentile non-recurring spending: ${total:>10.2f}")

def review_extraordinary_spendings(conn, year, month):
    print_divider("Reviewing Extraordinary Spendings")
    
    # Step 1: Get month summary
    df = db_operations.get_month_summary(conn, year, month)
    
    # Step 2: Order by (specified_month_sum - p50_monthly_sum) high to low
    df['difference'] = df['specified_month_sum'] - df['p50_monthly_sum']
    df_sorted = df.sort_values('difference', ascending=False)
    
    # Exclude specified categories
    excluded_categories = ['Rental income', 'Salary', 'Monthly fixed cost', 'Monthly property expense']
    df_sorted = df_sorted[~df_sorted['Category'].isin(excluded_categories)]
    
    # Step 3-5: Fetch transactions and store in memory
    extraordinary_transactions = defaultdict(list)
    
    for _, row in df_sorted.iterrows():
        category = row['Category']
        
        # Calculate P85 for this category and month
        p85 = db_operations.get_p85_for_category(conn, category, year, month)
        
        if p85 is None:
            continue  # Skip if no transactions for this category
        
        # Fetch transactions above P85
        transactions = db_operations.get_transactions_above_threshold(conn, category, year, month, p85)
        
        if not transactions.empty:
            extraordinary_transactions[category] = transactions.to_dict('records')
    
    # Step 6: Calculate P90 across all categories and filter transactions
    p90_amount = db_operations.get_p90_across_categories(conn, year, month, excluded_categories)
    
    # Filter out transactions less expensive than P90
    filtered_transactions = {}
    for category, transactions in extraordinary_transactions.items():
        filtered_transactions[category] = [t for t in transactions if abs(t['Amount']) >= p90_amount]
    
    # Step 7: Check for non-recurring transactions and filter
    non_recurring_transactions = {}
    for category, transactions in filtered_transactions.items():
        non_recurring_transactions[category] = []
        for transaction in transactions:
            # Check if this transaction is recurring
            count = db_operations.check_recurring_transaction(
                conn, 
                transaction['Description'],
                transaction['Amount'],
                transaction['Transaction Date']
            )
            
            if count == 0:  # This is a non-recurring transaction
                non_recurring_transactions[category].append(transaction)

    # Display results
    if any(non_recurring_transactions.values()):
        print(f"P90 amount across all categories: ${p90_amount:.2f}")
        print("\nExtraordinary non-recurring transactions:")
        total = 0
        for category, transactions in non_recurring_transactions.items():
            for transaction in transactions:
                date = transaction['Transaction Date'].strftime('%Y-%m-%d')
                amount = abs(transaction['Amount'])
                print(f"[{category:<20}] {date}\t{transaction['Description']:<40}\t${amount:>10.2f}")
                total += amount
        print(f"\nTotal extraordinary non-recurring spending: ${total:>10.2f}")
    else:
        print("No extraordinary non-recurring transactions found for this month.")

def set_budget(conn):
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    
    while True:
        print("\nCategories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        print(f"{len(categories) + 1}. Back to main menu")

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
            print_dataframe(df)
        except Exception as e:
            print(f"An error occurred: {str(e)}")

def add_adjustment_transaction(conn, year, month):
    print("\nAdding an adjustment transaction:")
    
    transaction_date = f"{year}-{month:02d}-01"
    description = input("Enter transaction description: ")
    amount = get_user_input("Enter amount (negative for expense, positive for income): ", float)
    categories = sorted(db_operations.get_global_categories_from_db(conn))
    print("\nCategories:")
    for i, category in enumerate(categories, 1):
        print(f"{i}. {category}")
    print(f"{len(categories) + 1}. Other")
    
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
        breakdown_id = db_operations.insert_surplus_deficit_breakdown(conn, description, breakdown, effective_date)
        calculate_and_conditionally_insert_monthly_breakdowns(conn, breakdown_id, breakdown, effective_date)
        print("Goal added successfully and monthly breakdowns calculated.")
    except Exception as e:
        print(f"Error adding goal or calculating monthly breakdowns: {str(e)}")

def get_goal_breakdown(conn):
    breakdown = {}
    remaining_percentage = 100
    categories = ['Investment', 'Savings'] + sorted(db_operations.get_global_categories_from_db(conn))

    # Ask for Investment and Savings first
    for category in ['Investment', 'Savings']:
        percentage = get_user_input(f"Enter percentage for {category} (0-{remaining_percentage}%): ", 
                                    float, lambda x: 0 <= x <= remaining_percentage)
        if percentage > 0:
            breakdown[category] = percentage / 100
            remaining_percentage -= percentage
        categories.remove(category)

    while remaining_percentage > 0:
        print("\nAvailable categories:")
        for i, category in enumerate(categories, 1):
            print(f"{i}. {category}")
        print(f"{len(categories) + 1}. Enter a new description")
        print(f"{len(categories) + 2}. Finish setting goals")

        choice = get_user_choice("\nChoose a category number or action: ", range(1, len(categories) + 3))
        
        if choice == len(categories) + 2:
            break
        elif choice == len(categories) + 1:
            description = input("Enter a new description: ")
            percentage = get_user_input(f"Enter percentage for this description (0-{remaining_percentage}%): ", 
                                        float, lambda x: 0 <= x <= remaining_percentage)
            if percentage > 0:
                breakdown[description] = percentage / 100
                remaining_percentage -= percentage
        else:
            selected_category = categories[choice - 1]
            percentage = get_user_input(f"Enter percentage for {selected_category} (0-{remaining_percentage}%): ", 
                                        float, lambda x: 0 <= x <= remaining_percentage)
            if percentage > 0:
                breakdown[selected_category] = percentage / 100
                remaining_percentage -= percentage
                categories.remove(selected_category)

        print(f"\nRemaining percentage: {remaining_percentage}%")

    if remaining_percentage > 0:
        print(f"Warning: {remaining_percentage}% of the budget was not allocated.")

    return breakdown

def calculate_and_conditionally_insert_monthly_breakdowns(conn, breakdown_id, breakdown, effective_date):
    valid_categories = set(db_operations.get_global_categories_from_db(conn))
    latest_transaction_date = db_operations.get_latest_transaction_date(conn)
    
    current_date = datetime.strptime(effective_date, '%Y-%m-%d').date()
    end_date = latest_transaction_date.replace(day=1) + relativedelta(months=1) - relativedelta(days=1)
    today = date.today()

    while current_date <= end_date:
        # Only process months that have ended
        if current_date.replace(day=1) + relativedelta(months=1) <= today:
            net_income = db_operations.get_net_income_for_month(conn, current_date.year, current_date.month)
            for category_or_description, percentage in breakdown.items():
                amount = net_income * percentage
                if category_or_description in valid_categories:
                    category = description = category_or_description
                else:
                    category, description = None, category_or_description
                db_operations.insert_surplus_deficit_breakdown_item(
                    conn, breakdown_id, category, description, amount, current_date
                )

        current_date += relativedelta(months=1)

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

def validate_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return False