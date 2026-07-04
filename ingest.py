import pandas as pd
import sys
import os
import shutil
from collections import defaultdict
import concurrent.futures
import threading
from datetime import datetime
from db_operations import (
    get_category_mapping_from_db,
    get_vendor_mapping_from_db,
    get_global_categories_from_db,
    persist_data_in_db,
    get_db_connection,
    insert_category,
    insert_category_matching_pattern,
    get_category_history,
)
from helpers import sort_categories

CATEGORY_GROUPS = ["Revenue", "Cost of revenue", "Non-discretionary", "Discretionary", "Misc"]

input_lock = threading.Lock()

# Folder where bank CSV exports land. Override with BUDGET_DOWNLOADS_DIR if needed.
DOWNLOADS_DIR = os.environ.get("BUDGET_DOWNLOADS_DIR", "/mnt/c/Users/00vin/Downloads")

def _select_from_list(prompt, options):
    while True:
        print(prompt)
        for i, option in enumerate(options, 1):
            print(f"{i}. {option}")
        try:
            choice = int(input("Enter the number of your choice: "))
            if 1 <= choice <= len(options):
                return options[choice - 1]
            else:
                print("Invalid choice. Please try again.")
        except ValueError:
            print("Invalid input. Please enter a number.")

def get_input_files(bank_type):
    files = []
    while True:
        file = input(f"Enter path to {bank_type} CSV file (or press Enter if done): ")
        if file == "":
            break
        if os.path.exists(file):
            files.append(file)
        else:
            print("File not found. Please try again.")
    return files

def currency_to_float(x):
    if pd.isna(x):
        return 0.0
    return float(str(x).replace('$', '').replace(',', ''))

def _maybe_save_match_rule(conn, description, category, category_map, unique_categories, is_new):
    """Offer to persist a keyword substring rule for a just-categorized vendor.

    Writes to category_matching_patterns so future imports auto-match this vendor
    (the substring matcher in apply_category_mapping/get_category) and injects the
    rule into the in-memory category_map so it also applies to the rest of this run.
    Skipped for EXCLUDE and when no DB connection is available (e.g. unit tests).
    A brand-new category is recorded in `categories` first to satisfy the FK.
    """
    if conn is None or category == "EXCLUDE":
        return

    raw = input(f"Save match rule? keyword [{description}] (Enter=accept, -=skip): ").strip()
    if raw == "-":
        return
    keyword = raw if raw else description
    if len(keyword) < 3:
        print("  Keyword too short — skipped (avoids over-matching).")
        return

    try:
        if is_new or category not in unique_categories:
            group = input(f"  New category '{category}' — group {CATEGORY_GROUPS} [default Misc]: ").strip()
            if group not in CATEGORY_GROUPS:
                group = "Misc"
            insert_category(conn, category, group)
            if category not in unique_categories:
                unique_categories.append(category)
        insert_category_matching_pattern(conn, keyword, category)
        conn.commit()
        category_map[keyword] = category  # apply for the remainder of this run
    except Exception as e:
        print(f"  Could not save match rule: {e}")

def _fmt_amount(amount):
    if amount is None:
        return ""
    return f"-${abs(amount):,.2f}" if amount < 0 else f"${amount:,.2f}"


def _save_exact_vendor_rule(conn, description, category, category_map, unique_categories):
    """Promote an exact-vendor suggestion to a permanent keyword rule (the 'a' shortcut).

    Uses the full description as the keyword (an exact-vendor match), so it only
    makes sense for description-sourced suggestions, never amount/check ones.
    """
    if conn is None:
        return
    try:
        insert_category_matching_pattern(conn, description, category)
        conn.commit()
        category_map[description] = category  # apply for the rest of this run
    except Exception as e:
        print(f"  Could not save rule: {e}")


def _history_suggestion(conn, description, amount, txn_type):
    """Return (suggested_category, source) from history, printing a reminder.

    source is 'description' or 'amount'. Returns (None, None) when there's no
    usable history; for a split (>=2 categories) it prints a breakdown + nudge
    and returns (None, source) so the caller offers no Enter-default.
    """
    if conn is None:
        return None, None
    try:
        hist = get_category_history(conn, description, amount, txn_type)
    except Exception:
        return None, None  # a history lookup must never block an import

    desc_hist, amt_hist = hist["by_description"], hist["by_amount"]
    primary, source = (desc_hist, "description") if desc_hist else (amt_hist, "amount")
    if not primary:
        return None, None

    label = f"'{description}'" if source == "description" else f"{_fmt_amount(amount)} {txn_type}"
    if len(primary) == 1:
        cat, cnt = primary[0]
        print(f"  \U0001F4A1 {label} seen {cnt}x before, all '{cat}'.")
        return cat, source
    breakdown = ", ".join(f"{c} x{n}" for c, n in primary)
    print(f"  ⚠ {label} is split: {breakdown} — consider cleaning this up later in the app.")
    return None, source

# this function prompts user for choice of category
def get_category(description, category_map, unique_categories, user_choices, conn=None, amount=None, txn_type=None):
    for key, value in category_map.items():
        if key.lower() in description.lower():
            return value, False  # False indicates no user intervention

    if description in user_choices:
        return user_choices[description], False  # False because this was a previous choice

    with input_lock:
        suffix = f"  ({_fmt_amount(amount)}, {txn_type})" if amount is not None else ""
        print(f"\nTransaction: {description}{suffix}")

        suggestion, source = _history_suggestion(conn, description, amount, txn_type)

        print("Choose a category or enter a new one:")

        # Sort categories (alphabetical, but with _SORT_AFTER pins like Business
        # revenue → after Salary), excluding "EXCLUDE"
        sorted_categories = sort_categories([cat for cat in unique_categories if cat != "EXCLUDE"])

        # Add "EXCLUDE" option at the end
        sorted_categories.append("EXCLUDE")

        for i, cat in enumerate(sorted_categories, 1):
            print(f"{i}. {cat}")
        print(f"{len(sorted_categories) + 1}. Enter a new category")

        if suggestion is not None:
            can_always = source == "description"
            prompt = (f"[Enter=accept '{suggestion}'"
                      + (", a=always make it a rule" if can_always else "")
                      + ", or a number]: ")
        else:
            prompt = "Enter the number of your choice: "

        while True:
            raw = input(prompt).strip()

            # Enter accepts the suggestion (no keyword rule saved — it's a confirm).
            if suggestion is not None and raw == "":
                user_choices[description] = suggestion
                return suggestion, True
            # 'a' = always: promote an exact-vendor suggestion to a permanent rule.
            if suggestion is not None and source == "description" and raw.lower() == "a":
                user_choices[description] = suggestion
                _save_exact_vendor_rule(conn, description, suggestion, category_map, unique_categories)
                return suggestion, True

            try:
                choice = int(raw)
            except ValueError:
                hint = " (or Enter to accept)" if suggestion is not None else ""
                print(f"Invalid input. Please enter a number{hint}.")
                continue

            if 1 <= choice <= len(sorted_categories):
                selected_category = sorted_categories[choice - 1]
                user_choices[description] = selected_category
                _maybe_save_match_rule(conn, description, selected_category,
                                       category_map, unique_categories, is_new=False)
                return selected_category, True  # True indicates user intervention
            elif choice == len(sorted_categories) + 1:
                new_category = input("Enter the new category: ").strip()
                user_choices[description] = new_category
                _maybe_save_match_rule(conn, description, new_category,
                                       category_map, unique_categories, is_new=True)
                return new_category, True  # True indicates user intervention
            else:
                print("Invalid choice. Please try again.")

def apply_category_mapping(description, vendor_map, keyword_map):
    if description in vendor_map:
        return vendor_map[description]
    for key, value in keyword_map.items():
        if key.lower() in description.lower():
            return value
    return None

def process_chase_csv(input_file, global_categories, user_choices, vendor_map, category_map, conn=None):
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading file '{input_file}': {e}")
        return None

    df = df[df['Description'] != "AUTOMATIC PAYMENT - THANK"].copy()
    df['Card'] = os.path.basename(input_file).split('_')[0]
    df['Memo'] = df.get('Memo', '').fillna('')

    for index, row in df.iterrows():
        mapped_category = apply_category_mapping(row['Description'], vendor_map, category_map)

        if mapped_category:
            old_category = df.at[index, 'Category']
            df.at[index, 'Category'] = mapped_category
            df.at[index, 'Memo'] += f' auto; was: {old_category}'
        elif pd.isna(row['Category']) or row['Category'] in ["Bills & Utilities", "Professional Services", "Personal", ""]:
            category, user_intervened = get_category(row['Description'], category_map, global_categories,
                                                     user_choices, conn, row['Amount'], row.get('Type'))
            if category == "EXCLUDE":
                df.at[index, 'Category'] = None
            else:
                df.at[index, 'Category'] = category
                if user_intervened:
                    df.at[index, 'Memo'] += ' manual'

    return df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]

def process_schwab_csv(input_file, global_categories, user_choices, vendor_map, category_map, conn=None):
    usecols = ['Date', 'Description', 'Type', 'Withdrawal', 'Deposit']
    try:
        df = pd.read_csv(input_file, usecols=usecols)
    except Exception as e:
        print(f"Error reading file '{input_file}': {e}")
        return None
    # filtering out Chase Credit card payments
    df = df[~df['Description'].str.contains('CHASE CREDIT', case=False, na=False)]
    df = df[df['Type'] != 'TRANSFER']

    df['Withdrawal'] = df['Withdrawal'].apply(currency_to_float)
    df['Deposit'] = df['Deposit'].apply(currency_to_float)

    df['Category'] = ''
    df['Amount'] = df['Deposit'] - df['Withdrawal']
    df['Memo'] = ''
    df['Transaction Date'] = df['Date']

    for index, row in df.iterrows():
        mapped_category = apply_category_mapping(row['Description'], vendor_map, category_map)

        if mapped_category:
            df.at[index, 'Category'] = mapped_category
            df.at[index, 'Memo'] += ' auto'
        else:
            category, user_intervened = get_category(row['Description'], category_map, global_categories,
                                                     user_choices, conn, row['Amount'], row['Type'])
            if category == "EXCLUDE":
                df.at[index, 'Category'] = None
            else:
                df.at[index, 'Category'] = category

    df['Card'] = 'Schwab'

    return df[['Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']]

def detect_bank_from_header(filepath):
    """Classify a CSV as 'chase', 'schwab', or None by reading its header row.

    Schwab exports carry a 'RunningBalance' column; Chase exports carry a
    'Post Date' column. Anything else is unrecognized and skipped.
    """
    try:
        with open(filepath, "r", newline="") as f:
            header = f.readline()
    except Exception:
        return None
    cols = [c.strip().strip('"') for c in header.split(",")]
    if "RunningBalance" in cols:
        return "schwab"
    if "Post Date" in cols:
        return "chase"
    return None


def _archive_file(filepath, archive_dir):
    """Move an imported file into archive_dir, avoiding name collisions."""
    os.makedirs(archive_dir, exist_ok=True)
    base = os.path.basename(filepath)
    dest = os.path.join(archive_dir, base)
    if os.path.exists(dest):
        root, ext = os.path.splitext(base)
        i = 1
        while os.path.exists(dest):
            dest = os.path.join(archive_dir, f"{root}_{i}{ext}")
            i += 1
    shutil.move(filepath, dest)
    return dest


def auto_import_from_downloads(conn, global_categories, user_choices, vendor_map, category_map):
    """Scan DOWNLOADS_DIR, auto-classify Chase/Schwab CSVs, import, then archive.

    Only top-level files are considered (the imported/ archive is skipped). Files
    that parse successfully are moved to DOWNLOADS_DIR/imported/ so the next run
    only sees genuinely new exports; a file that fails to parse is left in place.
    """
    if not os.path.isdir(DOWNLOADS_DIR):
        print(f"Downloads folder not found: {DOWNLOADS_DIR}")
        print("Set BUDGET_DOWNLOADS_DIR to point at your exports folder.")
        return

    archive_dir = os.path.join(DOWNLOADS_DIR, "imported")
    recognized, skipped = [], []
    for name in sorted(os.listdir(DOWNLOADS_DIR)):
        full = os.path.join(DOWNLOADS_DIR, name)
        if not os.path.isfile(full) or not name.lower().endswith(".csv"):
            continue
        bank = detect_bank_from_header(full)
        (recognized if bank else skipped).append((full, name, bank))

    if not recognized:
        print(f"No new Chase or Schwab CSVs found in {DOWNLOADS_DIR}.")
        if skipped:
            print("Ignored (unrecognized): " + ", ".join(n for _, n, _ in skipped))
        return

    print(f"\nFound {len(recognized)} bank CSV(s) in {DOWNLOADS_DIR}:")
    for _, name, bank in recognized:
        print(f"  {bank.capitalize():7} <- {name}")
    if skipped:
        print("Ignoring (unrecognized): " + ", ".join(n for _, n, _ in skipped))

    if input("\nImport these? [Enter=yes, q=cancel]: ").strip().lower() == "q":
        print("Cancelled.")
        return

    processors = {"chase": process_chase_csv, "schwab": process_schwab_csv}
    combined_df = pd.DataFrame()
    imported_files = []
    for full, name, bank in recognized:
        df = processors[bank](full, global_categories, user_choices, vendor_map, category_map, conn)
        if df is None:
            print(f"  ! Parse error, left in place: {name}")
            continue
        combined_df = pd.concat([combined_df, df], ignore_index=True)
        imported_files.append(full)

    if combined_df.empty:
        print("Nothing to import after processing.")
        return

    persist_data_in_db(conn, combined_df, "consolidated_transactions")
    # Commit before archiving so we never move a source file for unsaved data.
    conn.commit()

    for full in imported_files:
        dest = _archive_file(full, archive_dir)
        print(f"  archived -> imported/{os.path.basename(dest)}")
    print(f"Done. Imported {len(imported_files)} file(s); archived to {archive_dir}.")


def process_files_parallel(input_files, process_func, global_categories, user_choices, vendor_map, category_map, conn=None):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        processed_dfs = list(executor.map(lambda f: process_func(f, global_categories, user_choices, vendor_map, category_map, conn), input_files))

    processed_dfs = [df for df in processed_dfs if df is not None and not df.empty]
    return pd.concat(processed_dfs, ignore_index=True) if processed_dfs else None

def main():
    db_path = "budgeting-tool.db"
    conn = get_db_connection(db_path)
    category_map = get_category_mapping_from_db(conn)
    vendor_map = get_vendor_mapping_from_db(conn)
    global_categories = get_global_categories_from_db(conn)
    table_name = 'consolidated_transactions'

    user_choices = {}
    chase_files = []
    schwab_files = []

    while True:
        bank_choice = _select_from_list("Select import source:", [
            "Auto-import new CSVs from Downloads",
            "Chase (CSV)",
            "Charles Schwab (CSV)",
            "Done",
        ])
        if bank_choice == "Done":
            break
        elif bank_choice == "Auto-import new CSVs from Downloads":
            auto_import_from_downloads(conn, global_categories, user_choices, vendor_map, category_map)
        elif bank_choice == "Chase (CSV)":
            chase_files.extend(get_input_files("Chase"))
        elif bank_choice == "Charles Schwab (CSV)":
            schwab_files.extend(get_input_files("Charles Schwab"))

    combined_df = pd.DataFrame()

    if chase_files:
        chase_df = process_files_parallel(chase_files, process_chase_csv, global_categories, user_choices, vendor_map, category_map, conn)
        if chase_df is not None:
            combined_df = pd.concat([combined_df, chase_df], ignore_index=True)

    if schwab_files:
        schwab_df = process_files_parallel(schwab_files, process_schwab_csv, global_categories, user_choices, vendor_map, category_map, conn)
        if schwab_df is not None:
            combined_df = pd.concat([combined_df, schwab_df], ignore_index=True)

    if not combined_df.empty:
        persist_data_in_db(conn, combined_df, table_name)
    else:
        print("Error: No data to save. Please check your input files and try again.")

    conn.close()

if __name__ == "__main__":
    main()
