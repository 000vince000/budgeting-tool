# Budgeting Tool

A personal finance CLI built on embedded DuckDB. Import bank transactions, categorize them, set budgets, flag anomalies, and review spending patterns — all from a terminal menu.

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# First-time only: create schema and seed categories
python create_schema.py
python populate-seeddata-into-duckdb.py
```

## Usage

```bash
# Main interactive menu
python interaction.py

# Import transactions from Chase or Schwab CSVs
python ingest.py

# Run tests
python -m pytest tests/
```

## Menu Features

| Option | Description |
|---|---|
| See spending profile | Matplotlib charts: monthly spend by category, goal progress |
| See flagged transactions | Review and unflag previously flagged transactions |
| Search transactions by keyword | Full-text search across descriptions |
| Dig into a category | Browse transactions; recategorize, flag, amortize, or memo |
| Dig into a category group | Same as above, aggregated by group (Revenue, Discretionary, etc.) |
| See biggest one-off expenses | Top 15% most expensive non-recurring charges this month |
| Review extraordinary spendings | Categories that overspent their historical median; surfaces the culprit transactions using P85/P90 baselines |
| Set budget | Assign monthly budget to any category |
| Add an adjustment transaction | Manually insert a one-off transaction |
| Set goals | Allocate surplus by percentage across categories/descriptions |

## Architecture

```
interaction.py          ← CLI menu, entry point
    ├── transactions.py ← business logic (amortization, recategorization, analysis)
    │       └── db_operations.py ← all SQL / DuckDB reads and writes
    └── visualize-results.py ← matplotlib charts
```

**Database:** `budgeting-tool.db` (DuckDB, embedded, no server needed)

**Key tables:** `consolidated_transactions`, `categories`, `category_budgets`, `vendor_category_mapping`, `category_matching_patterns`, `surplus_and_deficit_breakdowns`, `surplus_and_deficit_breakdown_items`, `flagged_transactions`

**Key views:** `current_budgets`, `category_validation_view`, `top_15_vendors_view`

## Categorization

Ingestion auto-categorizes via two mechanisms (in priority order):
1. **Vendor mapping** (`vendor_category_mapping`) — exact vendor overrides
2. **Keyword patterns** (`category_matching_patterns`) — substring matches on description

Unknown vendors are prompted interactively during `ingest.py`. Recategorizing a transaction via the menu offers the option to apply the new category to all past and future transactions from that vendor, updating the mapping table.

## Data Flow

```
Chase/Schwab CSVs → ingest.py → consolidated_transactions
User actions → interaction.py → transactions.py → db_operations.py → DuckDB
DuckDB → visualize-results.py → PNG charts
```

## TODO

- **Make the unique index self-healing in `create_schema.py`.** Dedup on `consolidated_transactions` depends on a unique index over `(Card, Transaction Date, Description, Amount)`. Today that index is only created bundled with fresh table creation (`create_table_with_sequence` → `create_unique_index`), and `create_table` uses `CREATE TABLE IF NOT EXISTS` — so on a DB whose table already exists, the index is never backfilled and dedup silently does nothing. Change `create_unique_index` to `CREATE UNIQUE INDEX IF NOT EXISTS …` and ensure it runs against existing tables, so a rebuilt/older DB repairs itself.
