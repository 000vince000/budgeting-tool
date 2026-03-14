import unittest
import duckdb
from datetime import date, datetime

import pandas as pd

from db_operations import (
    add_memo_to_transaction,
    delete_vendor_category_mapping,
    check_recurring_transaction,
    fetch_transactions_by_category,
    fetch_transactions_by_categories,
    flag_transaction,
    get_actual_spending,
    get_biggest_oneoff_expenses,
    get_categories_with_groups_from_db,
    get_category_mapping_from_db,
    get_flagged_transactions,
    get_global_categories_from_db,
    get_latest_month,
    get_latest_transaction_date,
    get_net_income_for_month,
    get_p85_for_category,
    get_subtotal_by_category_group_for_month,
    get_transactions_by_vendor,
    get_vendor_category_mapping,
    get_vendor_mapping_from_db,
    insert_adjustment_transaction,
    insert_amortized_transaction,
    insert_category_budget,
    insert_vendor_category_mapping,
    persist_data_in_db,
    recategorize_transaction,
    recategorize_transactions,
    search_transactions_by_keyword,
    unflag_transaction,
    update_transaction_amount,
    update_transaction_memo,
)


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

def _create_schema(conn):
    conn.execute("CREATE SEQUENCE IF NOT EXISTS consolidated_transactions_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS consolidated_transactions (
            id BIGINT DEFAULT nextval('consolidated_transactions_id_seq') PRIMARY KEY,
            "Card" VARCHAR,
            "Transaction Date" DATE,
            "Description" VARCHAR,
            "Category" VARCHAR,
            "Type" VARCHAR,
            "Amount" DECIMAL(10, 2),
            "Memo" VARCHAR,
            UNIQUE ("Card", "Transaction Date", "Description", "Amount")
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            category VARCHAR PRIMARY KEY,
            category_group VARCHAR
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS category_budgets_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS category_budgets (
            id BIGINT DEFAULT nextval('category_budgets_id_seq') PRIMARY KEY,
            category VARCHAR,
            budget INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category) REFERENCES categories(category),
            UNIQUE (category)
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS vendor_category_mapping_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vendor_category_mapping (
            id BIGINT DEFAULT nextval('vendor_category_mapping_id_seq') PRIMARY KEY,
            vendor VARCHAR,
            category VARCHAR,
            FOREIGN KEY (category) REFERENCES categories(category),
            UNIQUE (vendor)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS category_matching_patterns (
            keyword VARCHAR PRIMARY KEY,
            category VARCHAR,
            FOREIGN KEY (category) REFERENCES categories(category)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS flagged_transactions (
            transaction_id BIGINT PRIMARY KEY,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (transaction_id) REFERENCES consolidated_transactions(id)
        )
    """)


def _insert_tx(conn, id, card, iso_date, description, category, type_='Sale', amount=-10.0, memo=''):
    conn.execute("""
        INSERT INTO consolidated_transactions
            (id, "Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo")
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (id, card, iso_date, description, category, type_, amount, memo))


# ---------------------------------------------------------------------------
# Category and vendor lookup reads
# ---------------------------------------------------------------------------

class TestCategoryAndVendorReads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        cls.conn.execute("INSERT INTO categories VALUES ('Entertainment', 'Discretionary')")
        cls.conn.execute("INSERT INTO category_matching_patterns VALUES ('amazon', 'Groceries')")
        cls.conn.execute("INSERT INTO category_matching_patterns VALUES ('netflix', 'Entertainment')")
        cls.conn.execute("INSERT INTO vendor_category_mapping (vendor, category) VALUES ('Whole Foods', 'Groceries')")

    def test_get_category_mapping_returns_dict(self):
        result = get_category_mapping_from_db(self.conn)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['amazon'], 'Groceries')
        self.assertEqual(result['netflix'], 'Entertainment')

    def test_get_vendor_mapping_returns_dict(self):
        result = get_vendor_mapping_from_db(self.conn)
        self.assertEqual(result['Whole Foods'], 'Groceries')

    def test_get_global_categories_returns_list(self):
        result = get_global_categories_from_db(self.conn)
        self.assertIn('Groceries', result)
        self.assertIn('Entertainment', result)

    def test_get_categories_with_groups_returns_tuples(self):
        result = get_categories_with_groups_from_db(self.conn)
        self.assertIn(('Groceries', 'Discretionary'), result)

    def test_get_vendor_category_mapping_hit(self):
        result = get_vendor_category_mapping(self.conn, 'Whole Foods')
        self.assertEqual(result, 'Groceries')

    def test_get_vendor_category_mapping_miss(self):
        result = get_vendor_category_mapping(self.conn, 'Unknown Store')
        self.assertIsNone(result)


# ---------------------------------------------------------------------------
# Transaction reads
# ---------------------------------------------------------------------------

class TestTransactionReads(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        cls.conn.execute("INSERT INTO categories VALUES ('Dining', 'Discretionary')")
        # Jan 2024 transactions
        _insert_tx(cls.conn, 1, 'Chase', '2024-01-05', 'Whole Foods', 'Groceries', amount=-50.0)
        _insert_tx(cls.conn, 2, 'Chase', '2024-01-10', 'Trader Joes', 'Groceries', amount=-30.0)
        _insert_tx(cls.conn, 3, 'Chase', '2024-01-15', 'Shake Shack', 'Dining', amount=-20.0)
        # Feb 2024 transaction
        _insert_tx(cls.conn, 4, 'Chase', '2024-02-01', 'Whole Foods', 'Groceries', amount=-60.0)

    def test_get_latest_month(self):
        result = get_latest_month(self.conn)
        self.assertEqual(result, date(2024, 2, 1))

    def test_get_latest_transaction_date(self):
        result = get_latest_transaction_date(self.conn)
        self.assertEqual(result, date(2024, 2, 1))

    def test_get_latest_transaction_date_empty_table_returns_today(self):
        conn = duckdb.connect(':memory:')
        _create_schema(conn)
        result = get_latest_transaction_date(conn)
        self.assertEqual(result, date.today())

    def test_get_net_income_for_month(self):
        result = get_net_income_for_month(self.conn, 2024, 1)
        self.assertAlmostEqual(float(result), -100.0)

    def test_get_net_income_excludes_null_category(self):
        # Transactions with NULL category should not be counted
        self.conn.execute("""
            INSERT INTO consolidated_transactions
                (id, "Card", "Transaction Date", "Description", "Category", "Type", "Amount")
            VALUES (99, 'Chase', '2024-01-20', 'Excluded', NULL, 'Sale', -999.0)
        """)
        result = get_net_income_for_month(self.conn, 2024, 1)
        self.assertAlmostEqual(float(result), -100.0)
        self.conn.execute("DELETE FROM consolidated_transactions WHERE id = 99")

    def test_fetch_transactions_by_category(self):
        result = fetch_transactions_by_category(self.conn, 'Groceries', 2024, 1)
        self.assertEqual(len(result), 2)
        self.assertTrue((result['Description'] == 'Whole Foods').any())

    def test_fetch_transactions_by_category_wrong_month(self):
        result = fetch_transactions_by_category(self.conn, 'Groceries', 2024, 3)
        self.assertTrue(result.empty)

    def test_fetch_transactions_by_categories(self):
        result = fetch_transactions_by_categories(self.conn, ['Groceries', 'Dining'], 2024, 1)
        self.assertEqual(len(result), 3)

    def test_get_actual_spending(self):
        result = get_actual_spending(self.conn, 2024, 1)
        groceries_row = result[result['Category'] == 'Groceries']
        self.assertAlmostEqual(float(groceries_row['actual_amount'].iloc[0]), -80.0)

    def test_check_recurring_transaction_found(self):
        # Whole Foods appears on 2024-01-05 and 2024-02-01 with the same amount
        count = check_recurring_transaction(self.conn, 'Whole Foods', -50.0, date(2024, 1, 5))
        # 2024-02-01 also has Whole Foods but with -60.0, so no exact amount match on other dates
        self.assertEqual(count, 0)

    def test_check_recurring_transaction_matches(self):
        count = check_recurring_transaction(self.conn, 'Whole Foods', -60.0, date(2024, 2, 1))
        # The Jan entry has -50.0, not -60.0, so no match
        self.assertEqual(count, 0)

    def test_get_transactions_by_vendor_partial_match(self):
        result = get_transactions_by_vendor(self.conn, 'Whole')
        self.assertGreaterEqual(len(result), 1)
        self.assertTrue((result['Description'] == 'Whole Foods').any())

    def test_search_transactions_by_keyword_description(self):
        result = search_transactions_by_keyword(self.conn, 'Shake', 2024, 1)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['Description'], 'Shake Shack')

    def test_search_transactions_by_keyword_no_match(self):
        result = search_transactions_by_keyword(self.conn, 'XYZNonexistent', 2024, 1)
        self.assertTrue(result.empty)

    def test_get_p85_for_category_excludes_current_month(self):
        # Jan 2024 has Groceries: -50, -30. Feb 2024 has: -60.
        # Asking for Jan's P85 should use only Feb data (all-time excl. current month) → P85 of [60] = 60.
        result = get_p85_for_category(self.conn, 'Groceries', 2024, 1)
        self.assertAlmostEqual(float(result), 60.0)

    def test_get_p85_for_category_uses_other_months(self):
        # Asking for Feb's P85 uses only Jan data: [-50, -30] → P85 = 47.0
        result = get_p85_for_category(self.conn, 'Groceries', 2024, 2)
        self.assertAlmostEqual(float(result), 47.0)

    def test_get_subtotal_by_category_group(self):
        result = get_subtotal_by_category_group_for_month(self.conn, 2024, 1)
        discretionary_row = result[result['category_group'] == 'Discretionary']
        self.assertAlmostEqual(float(discretionary_row['subtotal'].iloc[0]), -100.0)


# ---------------------------------------------------------------------------
# check_recurring_transaction
# ---------------------------------------------------------------------------

class TestCheckRecurringTransaction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        # Same vendor + same absolute amount on two different dates → genuinely recurring
        _insert_tx(cls.conn, 1, 'Chase', '2024-01-10', 'Netflix', 'Groceries', amount=-15.99)
        _insert_tx(cls.conn, 2, 'Chase', '2024-02-10', 'Netflix', 'Groceries', amount=-15.99)
        _insert_tx(cls.conn, 3, 'Chase', '2024-03-10', 'Netflix', 'Groceries', amount=-15.99)
        # Same vendor but different amounts → not recurring by amount
        _insert_tx(cls.conn, 4, 'Chase', '2024-01-05', 'Costco', 'Groceries', amount=-80.00)
        _insert_tx(cls.conn, 5, 'Chase', '2024-02-05', 'Costco', 'Groceries', amount=-120.00)

    def test_returns_count_of_other_matching_dates(self):
        # Jan Netflix: Feb and Mar also match → count = 2
        count = check_recurring_transaction(self.conn, 'Netflix', -15.99, date(2024, 1, 10))
        self.assertEqual(count, 2)

    def test_positive_and_negative_amount_treated_equally(self):
        # ABS(Amount) comparison means sign shouldn't matter
        count = check_recurring_transaction(self.conn, 'Netflix', 15.99, date(2024, 1, 10))
        self.assertEqual(count, 2)

    def test_no_match_when_amounts_differ(self):
        # Costco amounts vary — Jan entry should not match Feb
        count = check_recurring_transaction(self.conn, 'Costco', -80.00, date(2024, 1, 5))
        self.assertEqual(count, 0)

    def test_no_match_for_unknown_vendor(self):
        count = check_recurring_transaction(self.conn, 'UnknownVendor', -10.00, date(2024, 1, 1))
        self.assertEqual(count, 0)

    def test_same_month_different_day_not_counted(self):
        # A second Netflix row in the same month (different day) should not be counted —
        # same-month charges are not "recurring" from another month's perspective
        _insert_tx(self.conn, 6, 'Schwab', '2024-01-20', 'Netflix', 'Groceries', amount=-15.99)
        count = check_recurring_transaction(self.conn, 'Netflix', -15.99, date(2024, 1, 10))
        # Only Feb (id=2) and Mar (id=3) are in different months → count = 2
        self.assertEqual(count, 2)


# ---------------------------------------------------------------------------
# get_biggest_oneoff_expenses
# ---------------------------------------------------------------------------

class TestGetBiggestOneoffExpenses(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Dining', 'Discretionary')")

        # 20 cheap background transactions — anchors the percentile distribution so
        # the target transactions (at -90 and -100) clear PERCENT_RANK >= 0.85
        for i in range(20):
            _insert_tx(cls.conn, i + 1, 'Chase', f'2024-01-{i + 1:02d}', f'BkgVendor{i}', 'Dining', amount=-5.0)

        # Two charges: same vendor, same amount, same month but different days (date-fix scenario)
        _insert_tx(cls.conn, 21, 'Chase', '2024-01-21', 'DoubleCharge', 'Dining', amount=-100.0)
        _insert_tx(cls.conn, 22, 'Chase', '2024-01-22', 'DoubleCharge', 'Dining', amount=-100.0)

        # Recurring service — same vendor/amount in Jan and Feb
        _insert_tx(cls.conn, 23, 'Chase', '2024-01-23', 'RecurringService', 'Dining', amount=-80.0)
        _insert_tx(cls.conn, 24, 'Chase', '2024-02-23', 'RecurringService', 'Dining', amount=-80.0)

        # True one-off — appears only in Jan
        _insert_tx(cls.conn, 25, 'Chase', '2024-01-24', 'OneOffExpense', 'Dining', amount=-90.0)

    def _descriptions(self):
        df = get_biggest_oneoff_expenses(self.conn, 2024, 1)
        return set() if (df is None or df.empty) else set(df['Description'].tolist())

    def test_genuine_oneoff_appears(self):
        self.assertIn('OneOffExpense', self._descriptions())

    def test_recurring_charge_excluded(self):
        self.assertNotIn('RecurringService', self._descriptions())

    def test_same_month_different_day_not_treated_as_recurring(self):
        # Under the fixed logic (DATE_TRUNC != month), same-month charges don't
        # count as recurring even if they share the same vendor and amount
        self.assertIn('DoubleCharge', self._descriptions())


# ---------------------------------------------------------------------------
# Transaction writes
# ---------------------------------------------------------------------------

class TestTransactionWrites(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        cls.conn.execute("INSERT INTO categories VALUES ('Dining', 'Discretionary')")

    def _fetch(self, id):
        return self.conn.execute(
            'SELECT * FROM consolidated_transactions WHERE id = ?', [id]
        ).fetchone()

    def test_persist_data_in_db_inserts_rows(self):
        df = pd.DataFrame([{
            'Card': 'Chase', 'Transaction Date': '01/05/2024', 'Description': 'Starbucks',
            'Category': 'Dining', 'Type': 'Sale', 'Amount': -5.0, 'Memo': ''
        }])
        persist_data_in_db(self.conn, df, 'consolidated_transactions')
        result = self.conn.execute(
            "SELECT Description FROM consolidated_transactions WHERE Description = 'Starbucks'"
        ).fetchone()
        self.assertIsNotNone(result)

    def test_persist_data_in_db_rejects_duplicate(self):
        df = pd.DataFrame([{
            'Card': 'Chase', 'Transaction Date': '01/06/2024', 'Description': 'DupeTest',
            'Category': 'Dining', 'Type': 'Sale', 'Amount': -7.0, 'Memo': ''
        }])
        persist_data_in_db(self.conn, df, 'consolidated_transactions')
        persist_data_in_db(self.conn, df, 'consolidated_transactions')  # duplicate
        count = self.conn.execute(
            "SELECT COUNT(*) FROM consolidated_transactions WHERE Description = 'DupeTest'"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_insert_adjustment_transaction(self):
        insert_adjustment_transaction(self.conn, date(2024, 3, 1), 'Manual Adjustment', -100.0, 'Groceries')
        result = self.conn.execute(
            "SELECT Amount FROM consolidated_transactions WHERE Description = 'Manual Adjustment'"
        ).fetchone()
        self.assertIsNotNone(result)
        self.assertAlmostEqual(float(result[0]), -100.0)

    def test_recategorize_transaction_changes_category(self):
        _insert_tx(self.conn, 200, 'Chase', '2024-04-01', 'Amazon', 'Dining', amount=-12.0)
        recategorize_transaction(self.conn, 200, 'Groceries', 'Dining')
        row = self.conn.execute(
            "SELECT Category FROM consolidated_transactions WHERE id = 200"
        ).fetchone()
        self.assertEqual(row[0], 'Groceries')

    def test_recategorize_transaction_appends_memo(self):
        _insert_tx(self.conn, 201, 'Chase', '2024-04-02', 'Target', 'Dining', amount=-15.0, memo='original')
        recategorize_transaction(self.conn, 201, 'Groceries', 'Dining')
        row = self.conn.execute(
            "SELECT Memo FROM consolidated_transactions WHERE id = 201"
        ).fetchone()
        self.assertIn('Recategorized', row[0])

    def test_recategorize_transaction_empty_memo(self):
        _insert_tx(self.conn, 202, 'Chase', '2024-04-03', 'Costco', 'Dining', amount=-20.0, memo='')
        recategorize_transaction(self.conn, 202, 'Groceries', 'Dining')
        row = self.conn.execute(
            "SELECT Memo FROM consolidated_transactions WHERE id = 202"
        ).fetchone()
        self.assertIn('Recategorized', row[0])

    def test_recategorize_transactions_batch(self):
        _insert_tx(self.conn, 210, 'Chase', '2024-05-01', 'BatchA', 'Dining', amount=-5.0)
        _insert_tx(self.conn, 211, 'Chase', '2024-05-02', 'BatchB', 'Dining', amount=-6.0)
        recategorize_transactions(self.conn, [210, 211], 'Groceries')
        rows = self.conn.execute(
            "SELECT Category FROM consolidated_transactions WHERE id IN (210, 211)"
        ).fetchall()
        self.assertTrue(all(r[0] == 'Groceries' for r in rows))

    def test_update_transaction_amount(self):
        _insert_tx(self.conn, 300, 'Chase', '2024-06-01', 'UpdateAmount', 'Groceries', amount=-10.0)
        update_transaction_amount(self.conn, 300, -99.0)
        row = self.conn.execute(
            "SELECT Amount FROM consolidated_transactions WHERE id = 300"
        ).fetchone()
        self.assertAlmostEqual(float(row[0]), -99.0)

    def test_update_transaction_memo(self):
        _insert_tx(self.conn, 301, 'Chase', '2024-06-02', 'UpdateMemo', 'Groceries', amount=-10.0)
        update_transaction_memo(self.conn, 301, 'new memo')
        row = self.conn.execute(
            "SELECT Memo FROM consolidated_transactions WHERE id = 301"
        ).fetchone()
        self.assertEqual(row[0], 'new memo')

    def test_add_memo_to_transaction(self):
        _insert_tx(self.conn, 302, 'Chase', '2024-06-03', 'AddMemo', 'Groceries', amount=-10.0, memo='base')
        add_memo_to_transaction(self.conn, 302, ' added')
        row = self.conn.execute(
            "SELECT Memo FROM consolidated_transactions WHERE id = 302"
        ).fetchone()
        self.assertEqual(row[0], 'base added')

    def test_insert_amortized_transaction(self):
        result_id = insert_amortized_transaction(
            self.conn, 400, 'Chase', date(2024, 7, 1), 'Amortized', 'Groceries', -25.0, 'amort memo'
        )
        self.assertEqual(result_id, 400)
        row = self.conn.execute(
            "SELECT Description FROM consolidated_transactions WHERE id = 400"
        ).fetchone()
        self.assertEqual(row[0], 'Amortized')


# ---------------------------------------------------------------------------
# Flagging
# ---------------------------------------------------------------------------

class TestFlaggedTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        _insert_tx(cls.conn, 1, 'Chase', '2024-01-01', 'Flaggable', 'Groceries', amount=-10.0)

    def test_flag_and_retrieve(self):
        flag_transaction(self.conn, 1)
        result = get_flagged_transactions(self.conn)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['Description'], 'Flaggable')

    def test_unflag_removes_entry(self):
        # Ensure flagged first
        try:
            flag_transaction(self.conn, 1)
        except Exception:
            pass  # already flagged
        unflag_transaction(self.conn, 1)
        result = get_flagged_transactions(self.conn)
        self.assertTrue(result.empty)


# ---------------------------------------------------------------------------
# Mapping and budget writes
# ---------------------------------------------------------------------------

class TestMappingAndBudgetWrites(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.conn = duckdb.connect(':memory:')
        _create_schema(cls.conn)
        cls.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")

    def test_insert_vendor_category_mapping_valid(self):
        insert_vendor_category_mapping(self.conn, 'Safeway', 'Groceries')
        result = get_vendor_category_mapping(self.conn, 'Safeway')
        self.assertEqual(result, 'Groceries')

    def test_insert_vendor_category_mapping_invalid_category_raises(self):
        with self.assertRaises(ValueError):
            insert_vendor_category_mapping(self.conn, 'SomeVendor', 'NonexistentCategory')

    def test_insert_vendor_category_mapping_upserts_existing(self):
        # First insert
        insert_vendor_category_mapping(self.conn, 'UpsertVendor', 'Groceries')
        # Re-insert with same vendor — should update, not raise
        self.conn.execute("INSERT INTO categories VALUES ('Dining', 'Discretionary')")
        insert_vendor_category_mapping(self.conn, 'UpsertVendor', 'Dining')
        result = get_vendor_category_mapping(self.conn, 'UpsertVendor')
        self.assertEqual(result, 'Dining')

    def test_delete_vendor_category_mapping(self):
        insert_vendor_category_mapping(self.conn, 'DeleteVendor', 'Groceries')
        delete_vendor_category_mapping(self.conn, 'DeleteVendor')
        result = get_vendor_category_mapping(self.conn, 'DeleteVendor')
        self.assertIsNone(result)

    def test_insert_category_budget_valid(self):
        insert_category_budget(self.conn, 'Groceries', 500)
        result = self.conn.execute(
            "SELECT budget FROM category_budgets WHERE category = 'Groceries'"
        ).fetchone()
        self.assertEqual(result[0], 500)

    def test_insert_category_budget_invalid_category_raises(self):
        with self.assertRaises(Exception):
            insert_category_budget(self.conn, 'FakeCategory', 200)


if __name__ == '__main__':
    unittest.main()
