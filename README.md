# Budgeting Tool

This is a comprehensive budgeting tool that helps you manage your finances by tracking expenses, setting budgets, and visualizing spending patterns.

## Features

- Import transactions from Chase and Charles Schwab CSV files
- Categorize transactions automatically and manually
- Set and manage budgets for different categories
- Visualize spending patterns with charts
- Store data in a DuckDB database for efficient querying

## Files and Their Functions

### Main Scripts

1. `interaction.py`: The main interface for user interaction. It provides a menu to view spending profiles and set budgets.

2. `ingest.py`: Handles the import of transaction data from CSV files. It processes both Chase and Charles Schwab formats.

3. `visualize-results.py`: Creates visualizations of spending data using matplotlib.

4. `create-schema.py`: Sets up the database schema, creating necessary tables and views.

### Database Operations

5. `db_operations.py`: Contains functions for database operations like querying, inserting data, and retrieving category information.

6. `populate-seeddata-into-duckdb.py`: Populates the database with initial seed data for categories and category matching patterns.

7. `bulk-insert-csv-into-duckdb.py`: Provides functionality to bulk insert data from a CSV file into the DuckDB database.

### SQL Queries

8. `latest-month-summary.sql`: SQL query to generate a summary of the latest month's spending, including budget comparisons.

9. `current-budgets.sql`: SQL query to retrieve current budgets for each category.

## Setup and Usage

1. Ensure you have Python 3.x installed along with the required libraries (duckdb, pandas, matplotlib).

2. Run `create-schema.py` to set up the database structure.

3. Use `populate-seeddata-into-duckdb.py` to add initial category data.

4. Run `interaction.py` to start the main application interface.

5. Use the "Set budget" option to set budgets for different categories.

6. Use `ingest.py` to import transaction data from your bank CSV files.

7. Use the "See latest month's spending profile" option to visualize your spending patterns.

## Data Flow

1. Bank CSV files → `ingest.py` → DuckDB database
2. User input → `interaction.py` → DuckDB database
3. DuckDB database → `visualize-results.py` → Spending charts

## Customization

You can customize category mappings and global categories by modifying the seed data in `populate-seeddata-into-duckdb.py`.

## Note

This tool is designed for personal use and may require modifications to work with different bank CSV formats or to meet specific budgeting needs.
