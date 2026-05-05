#!/usr/bin/env python3
"""Fetch Chase transactions via Plaid (Development tier).

Prerequisites:
    1. Sign up at dashboard.plaid.com, create a Development app
    2. Complete the Chase security questionnaire in the Plaid dashboard
    3. Add to .env:  PLAID_CLIENT_ID=<id>  PLAID_SECRET=<development secret>
    4. pip install plaid-python flask python-dotenv
    5. python fetch_chase_plaid.py --link   (one-time; re-run if access token is revoked)

Usage:
    python fetch_chase_plaid.py --link                  # Link Chase account (opens browser)
    python fetch_chase_plaid.py --fetch                 # Fetch last 30 days, print rows
    python fetch_chase_plaid.py --fetch --month 2025-01 # Fetch specific month, print rows
"""
import argparse
import os
import sys
import threading
import webbrowser
from datetime import date, datetime, timedelta

import pandas as pd
from dotenv import load_dotenv

import token_store

load_dotenv()

PLAID_ENV = "production"


def _plaid_client():
    try:
        import plaid
        from plaid.api import plaid_api
        from plaid.api_client import ApiClient
        from plaid.configuration import Configuration
    except ImportError:
        print("Error: plaid-python is not installed. Run: pip install plaid-python")
        sys.exit(1)

    client_id = os.environ.get("PLAID_CLIENT_ID")
    secret = os.environ.get("PLAID_SECRET")
    if not client_id or not secret:
        print("Error: PLAID_CLIENT_ID and PLAID_SECRET must be set in .env")
        sys.exit(1)

    env_map = {
        "sandbox": plaid.Environment.Sandbox,
        "production": plaid.Environment.Production,
    }
    config = Configuration(
        host=env_map[PLAID_ENV],
        api_key={"clientId": client_id, "secret": secret},
    )
    return plaid_api.PlaidApi(ApiClient(config))


def link_account():
    """Start a local Flask server to handle the Plaid Link OAuth flow."""
    try:
        from flask import Flask, jsonify, request
    except ImportError:
        print("Error: flask is not installed. Run: pip install flask")
        sys.exit(1)

    from plaid.model.country_code import CountryCode
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    from plaid.model.link_token_create_request import LinkTokenCreateRequest
    from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
    from plaid.model.products import Products

    client = _plaid_client()

    link_resp = client.link_token_create(LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id="budgeting-tool-user"),
        client_name="Budgeting Tool",
        products=[Products("transactions")],
        country_codes=[CountryCode("US")],
        language="en",
    ))
    link_token = link_resp["link_token"]

    # Minimal HTML page that runs Plaid Link and POSTs the public_token back.
    link_html = f"""<!DOCTYPE html>
<html><head><title>Link Chase Account</title></head>
<body>
<h2>Linking your Chase account&hellip;</h2>
<p>Complete the prompts in the popup, then this page will close automatically.</p>
<script src="https://cdn.plaid.com/link/v2/stable/link-initialize.js"></script>
<script>
var handler = Plaid.create({{
  token: "{link_token}",
  onSuccess: function(public_token, metadata) {{
    fetch('/exchange', {{
      method: 'POST',
      headers: {{'Content-Type': 'application/json'}},
      body: JSON.stringify({{public_token: public_token}})
    }}).then(function() {{
      document.body.innerHTML = '<h2>Chase linked successfully. You can close this window.</h2>';
      setTimeout(function() {{ fetch('/shutdown', {{method: 'POST'}}); }}, 1500);
    }});
  }},
  onExit: function(err, metadata) {{
    document.body.innerHTML = '<h2>Linking cancelled.</h2>';
    setTimeout(function() {{ fetch('/shutdown', {{method: 'POST'}}); }}, 1500);
  }}
}});
handler.open();
</script>
</body></html>"""

    app = Flask(__name__)

    @app.route("/")
    def index():
        return link_html

    @app.route("/exchange", methods=["POST"])
    def exchange():
        public_token = request.json["public_token"]
        exchange_resp = client.item_public_token_exchange(
            ItemPublicTokenExchangeRequest(public_token=public_token)
        )
        token_store.save("plaid_chase", {
            "access_token": exchange_resp["access_token"],
            "item_id": exchange_resp["item_id"],
        })
        print("Chase account linked successfully.")
        return jsonify({"status": "ok"})

    @app.route("/shutdown", methods=["POST"])
    def shutdown():
        threading.Timer(0.5, lambda: os._exit(0)).start()
        return jsonify({"status": "shutting down"})

    port = 8080
    print(f"Opening browser at http://localhost:{port} — complete the Chase linking flow.")
    threading.Timer(1.0, lambda: webbrowser.open(f"http://localhost:{port}")).start()
    app.run(port=port, debug=False, use_reloader=False)


def fetch_transactions(start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch Chase transactions for the given date range via Plaid /transactions/get.

    Returns a DataFrame with columns:
        Card, Transaction Date (MM/DD/YYYY string), Description,
        Category, Type, Amount, Memo
    Amount follows the existing convention: negative = spending, positive = income.
    Plaid reports debits as positive amounts, so they are negated here.
    """
    from plaid.model.transactions_get_request import TransactionsGetRequest
    from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

    stored = token_store.load("plaid_chase")
    if not stored:
        print("No Chase account linked. Run: python fetch_chase_plaid.py --link")
        return pd.DataFrame(columns=[
            "Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo"
        ])

    client = _plaid_client()
    access_token = stored["access_token"]

    all_transactions = []
    offset = 0
    while True:
        resp = client.transactions_get(TransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=TransactionsGetRequestOptions(offset=offset, count=500),
        ))
        txns = resp["transactions"]
        all_transactions.extend(txns)
        if len(all_transactions) >= resp["total_transactions"]:
            break
        offset += len(txns)

    rows = []
    for txn in all_transactions:
        # Skip pending transactions — wait until they post.
        if txn.get("pending"):
            continue
        # Plaid amount: positive = debit (money out). Negate to match our convention.
        amount = -float(txn["amount"])
        txn_date = txn["date"]
        if isinstance(txn_date, str):
            dt = datetime.strptime(txn_date, "%Y-%m-%d")
        else:
            dt = datetime.combine(txn_date, datetime.min.time())
        rows.append({
            "Card": "Chase",
            "Transaction Date": dt.strftime("%m/%d/%Y"),
            "Description": txn.get("name", ""),
            "Category": "",
            "Type": txn.get("transaction_type", ""),
            "Amount": amount,
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
    parser = argparse.ArgumentParser(description="Fetch Chase transactions via Plaid")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--link", action="store_true", help="Link Chase account via Plaid Link")
    group.add_argument("--fetch", action="store_true", help="Fetch transactions and print them")
    parser.add_argument("--month", help="Month to fetch in YYYY-MM format (default: last 30 days)")
    args = parser.parse_args()

    if args.link:
        link_account()
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
