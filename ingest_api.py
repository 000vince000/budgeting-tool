"""API-based transaction ingestion: Chase (Plaid).

This module owns the full fetch→categorize→persist pipeline for API sources.
It is called from ingest.py's menu but has no module-level dependency on it —
`get_category` and `apply_category_mapping` are imported lazily inside functions
to avoid a circular import (ingest.py will import this module at load time).
"""
import sys
from datetime import date, timedelta

import pandas as pd

from db_operations import persist_data_in_db

STANDARD_COLUMNS = ["Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo"]


def _month_range(month_str: str) -> tuple[date, date]:
    """Return (first_day, last_day) for a 'YYYY-MM' string."""
    year, month = map(int, month_str.split("-"))
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def prompt_month() -> tuple[date, date]:
    """Ask for a YYYY-MM month and return (start_date, end_date)."""
    today = date.today()
    default = f"{today.year}-{today.month:02d}"
    raw = input(f"Month to fetch (YYYY-MM) [default: {default}]: ").strip() or default
    try:
        return _month_range(raw)
    except (ValueError, IndexError):
        print("Invalid format — using current month.")
        return _month_range(default)


def _apply_category_matching(
    df: pd.DataFrame,
    vendor_map: dict,
    category_map: dict,
    global_categories: list,
    user_choices: dict,
) -> pd.DataFrame:
    """Apply vendor/keyword category matching to a pre-built DataFrame.

    Uses apply_category_mapping and get_category from ingest.py (lazy import).
    Rows where the user picks EXCLUDE have Category set to None.
    Returns the DataFrame restricted to STANDARD_COLUMNS.
    """
    from ingest import apply_category_mapping, get_category  # lazy: avoids circular import

    for index, row in df.iterrows():
        mapped = apply_category_mapping(row["Description"], vendor_map, category_map)
        if mapped:
            df.at[index, "Category"] = mapped
            df.at[index, "Memo"] = (df.at[index, "Memo"] or "") + " auto"
        else:
            category, user_intervened = get_category(
                row["Description"], category_map, global_categories, user_choices
            )
            if category == "EXCLUDE":
                df.at[index, "Category"] = None
            else:
                df.at[index, "Category"] = category
                if user_intervened:
                    df.at[index, "Memo"] = (df.at[index, "Memo"] or "") + " manual"

    return df[STANDARD_COLUMNS]


def ingest_from_chase_plaid(conn, global_categories, user_choices, vendor_map, category_map):
    """Fetch Chase transactions via Plaid and persist them.

    Prereqs: PLAID_CLIENT_ID + PLAID_SECRET in .env,
             plaid-python + flask installed, account linked via: python fetch_chase_plaid.py --link
    """
    try:
        import fetch_chase_plaid
    except ImportError:
        print("plaid-python is not installed. Run: pip install plaid-python flask python-dotenv")
        return

    start, end = prompt_month()
    print(f"Fetching Chase transactions {start} → {end}…")
    try:
        df = fetch_chase_plaid.fetch_transactions(start, end)
    except Exception as e:
        print(f"Chase/Plaid fetch failed: {e}")
        return

    if df.empty:
        print("No transactions returned. Chase linked? Run: python fetch_chase_plaid.py --link")
        return

    print(f"  {len(df)} transactions. Applying category matching…")
    df = _apply_category_matching(df, vendor_map, category_map, global_categories, user_choices)
    persist_data_in_db(conn, df, "consolidated_transactions")
