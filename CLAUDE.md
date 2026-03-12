# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Activate virtual environment
source .venv/bin/activate

# Run main application
python interaction.py

# Import bank transactions (Chase/Schwab CSVs)
python ingest.py

# Set up database schema (first-time setup)
python create_schema.py

# Seed initial categories and vendor mappings
python populate-seeddata-into-duckdb.py

# Run unit tests
python -m unittest test_transactions.py
```

## Architecture

**Entry point:** `interaction.py` — interactive CLI menu that orchestrates all features.

**Layer structure:**
- `interaction.py` → calls `transactions.py` (business logic) and `visualize-results.py` (charts)
- `transactions.py` → calls `db_operations.py` (all DB reads/writes)
- `db_operations.py` → DuckDB via `duckdb.connect("budgeting-tool.db")`

**Key modules:**
- `db_operations.py` — all SQL queries and database interactions
- `transactions.py` — business logic: budgets, goals, amortization, vendor remapping, spending analysis
- `ingest.py` — parses Chase and Schwab CSV formats, matches vendors to categories, handles interactive categorization for unknowns
- `visualize-results.py` — matplotlib charts for monthly spending comparisons and goal progress
- `helpers.py` — shared utilities for user input, currency formatting, date parsing

**Database:** Embedded DuckDB file (`budgeting-tool.db`). No server required. Schema is defined in `create_schema.py`.

**Key tables:** `consolidated_transactions`, `categories`, `category_budgets`, `vendor_category_mapping`, `category_matching_patterns`, `surplus_and_deficit_breakdowns`, `surplus_and_deficit_breakdown_items`, `flagged_transactions`

**Views:** `current_budgets`, `category_validation_view`, `top_15_vendors_view`

## Data Flow

```
Bank CSVs (Chase/Schwab) → ingest.py → consolidated_transactions table
User actions via interaction.py → transactions.py → db_operations.py → DuckDB
DuckDB → visualize-results.py → PNG charts
```

## Working Style

Before finalizing any suggestion — especially fixes, refactors, or design changes — internally critique it first:
- Does this solution introduce a logical or semantic inconsistency?
- Are there different cases being conflated that should be handled separately?
- Would this break something that currently works correctly?

If the answer to any of these is "maybe", flag it to the user before proposing the solution, not after.

## Category System

Categories are grouped into: Revenue, Cost of revenue, Non-discretionary, Discretionary, Misc. The `category_matching_patterns` table stores keyword → category mappings used during ingestion. `vendor_category_mapping` stores vendor-level overrides that persist across imports.
