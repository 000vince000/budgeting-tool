#!/usr/bin/env python3
"""Fetch Schwab transactions via the Schwab developer API (developer.schwab.com).

Prerequisites:
    1. Register at developer.schwab.com, create an app, set callback URL to https://127.0.0.1
    2. Add to .env:  SCHWAB_CLIENT_ID=<app key>  SCHWAB_CLIENT_SECRET=<app secret>
    3. pip install schwab-py python-dotenv
    4. python fetch_schwab.py --auth   (once; repeat when token expires after 7 days)

Usage:
    python fetch_schwab.py --auth                  # OAuth flow (opens browser)
    python fetch_schwab.py --fetch                 # Fetch last 30 days, print rows
    python fetch_schwab.py --fetch --month 2025-01 # Fetch specific month, print rows
"""
import argparse
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

TOKEN_PATH = Path.home() / ".budgeting_schwab_token.json"
CALLBACK_URL = "https://127.0.0.1"

# Activity types that represent brokerage operations, not day-to-day spending.
_EXCLUDE_ACTIVITY_TYPES = {
    "TRADE",
    "RECEIVE_AND_DELIVER",
    "JOURNAL",
    "DIVIDEND_OR_INTEREST",
}


def _credentials():
    api_key = os.environ.get("SCHWAB_CLIENT_ID")
    app_secret = os.environ.get("SCHWAB_CLIENT_SECRET")
    if not api_key or not app_secret:
        print("Error: SCHWAB_CLIENT_ID and SCHWAB_CLIENT_SECRET must be set in .env")
        sys.exit(1)
    return api_key, app_secret


def get_client():
    """Return an authenticated schwab-py client. Opens browser if re-auth is needed."""
    import schwab

    api_key, app_secret = _credentials()
    if TOKEN_PATH.exists():
        try:
            return schwab.auth.client_from_token_file(str(TOKEN_PATH), api_key, app_secret)
        except Exception:
            pass
    print("No valid Schwab token found. A browser window will open for authentication.")
    return schwab.auth.easy_client(api_key, app_secret, CALLBACK_URL, str(TOKEN_PATH))


def fetch_transactions(start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch Schwab transactions for the given date range.

    Returns a DataFrame with columns:
        Card, Transaction Date (MM/DD/YYYY string), Description,
        Category, Type, Amount, Memo
    Amount follows the existing convention: negative = spending, positive = income.
    """
    client = get_client()

    resp = client.get_account_numbers()
    resp.raise_for_status()
    accounts = resp.json()

    rows = []
    for account in accounts:
        account_hash = account["hashValue"]
        resp = client.get_transactions(
            account_hash,
            start_date=datetime.combine(start_date, datetime.min.time()),
            end_date=datetime.combine(end_date, datetime.max.time()),
        )
        resp.raise_for_status()
        for txn in resp.json():
            activity_type = txn.get("activityType", "")
            if activity_type in _EXCLUDE_ACTIVITY_TYPES:
                continue
            description = txn.get("description", "")
            if "CHASE CREDIT" in description.upper():
                continue
            time_str = txn.get("time", "")
            if not time_str:
                continue
            dt = datetime.fromisoformat(time_str[:10])
            rows.append({
                "Card": "Schwab",
                "Transaction Date": dt.strftime("%m/%d/%Y"),
                "Description": description,
                "Category": "",
                "Type": activity_type,
                "Amount": txn.get("netAmount", 0),
                "Memo": "",
            })

    return pd.DataFrame(rows, columns=[
        "Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo"
    ])


def _month_range(month_str: str) -> tuple[date, date]:
    year, month = map(int, month_str.split("-"))
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(year, month + 1, 1) - timedelta(days=1)
    return start, end


def main():
    parser = argparse.ArgumentParser(description="Fetch Schwab transactions via developer API")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--auth", action="store_true", help="Run OAuth flow (opens browser)")
    group.add_argument("--fetch", action="store_true", help="Fetch transactions and print them")
    parser.add_argument("--month", help="Month to fetch in YYYY-MM format (default: last 30 days)")
    args = parser.parse_args()

    if args.auth:
        if TOKEN_PATH.exists():
            TOKEN_PATH.unlink()
        get_client()
        print(f"Authentication complete. Token saved to {TOKEN_PATH}")
        return

    if args.month:
        start, end = _month_range(args.month)
    else:
        end = date.today()
        start = end - timedelta(days=30)

    df = fetch_transactions(start, end)
    print(f"Fetched {len(df)} transactions ({start} to {end})")
    if not df.empty:
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
