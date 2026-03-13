import unittest
import duckdb
from datetime import date
from unittest.mock import patch

import pandas as pd
from dateutil.relativedelta import relativedelta

from transactions import amortize_transaction, recategorize_all_vendor_transactions
from test_db_operations import _create_schema, _insert_tx


# ---------------------------------------------------------------------------
# amortize_transaction
# ---------------------------------------------------------------------------

class TestAmortizeTransaction(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(':memory:')
        _create_schema(self.conn)
        self.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        _insert_tx(self.conn, 1, 'Chase', '2024-01-15', 'Big Purchase', 'Groceries', amount=-300.0, memo='')
        # Advance the sequence past the explicitly-inserted id so amortization gets fresh ids
        self.conn.execute("SELECT nextval('consolidated_transactions_id_seq')")

    def _df(self):
        return pd.DataFrame([{
            'id': 1, 'Card': 'Chase',
            'Transaction Date': date(2024, 1, 15),
            'Description': 'Big Purchase', 'Category': 'Groceries',
            'Amount': -300.0, 'memo': ''
        }])

    def _fetch_all(self):
        return self.conn.execute(
            "SELECT id, Amount, Memo, \"Transaction Date\" FROM consolidated_transactions ORDER BY id"
        ).fetchall()

    def test_monthly_amount_is_original_divided_by_n(self):
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        rows = self._fetch_all()
        for row in rows:
            self.assertAlmostEqual(float(row[1]), -100.0)

    def test_creates_n_minus_one_new_transactions(self):
        with patch('transactions.get_user_input', side_effect=[1, 4]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM consolidated_transactions"
        ).fetchone()[0]
        self.assertEqual(count, 4)

    def test_new_transaction_dates_increment_monthly(self):
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        dates = [row[3] for row in self._fetch_all()]
        self.assertEqual(dates[0], date(2024, 1, 15))
        self.assertEqual(dates[1], date(2024, 2, 15))
        self.assertEqual(dates[2], date(2024, 3, 15))

    def test_original_memo_updated_with_fraction(self):
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        original_memo = self._fetch_all()[0][2]
        self.assertIn('1/3', original_memo)
        self.assertIn('amortized', original_memo)

    def test_new_transaction_memos_reference_original_id(self):
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        rows = self._fetch_all()
        for row in rows[1:]:
            self.assertIn('Original transaction ID: 1', row[2])

    def test_new_transaction_memos_have_correct_fraction(self):
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        rows = self._fetch_all()
        self.assertIn('2/3', rows[1][2])
        self.assertIn('3/3', rows[2][2])

    def test_single_month_creates_no_new_rows(self):
        with patch('transactions.get_user_input', side_effect=[1, 1]):
            amortize_transaction(self.conn, self._df(), 2024, 1)
        count = self.conn.execute(
            "SELECT COUNT(*) FROM consolidated_transactions"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_rollback_on_failure(self):
        """If something goes wrong mid-amortization, original transaction is unchanged."""
        original_amount = -300.0
        with patch('transactions.get_user_input', side_effect=[1, 3]):
            with patch('db_operations.insert_amortized_transaction', side_effect=Exception("forced")):
                amortize_transaction(self.conn, self._df(), 2024, 1)
        rows = self._fetch_all()
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0][1]), original_amount)


# ---------------------------------------------------------------------------
# recategorize_all_vendor_transactions
# ---------------------------------------------------------------------------

class TestRecategorizeAllVendorTransactions(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(':memory:')
        _create_schema(self.conn)
        self.conn.execute("INSERT INTO categories VALUES ('Groceries', 'Discretionary')")
        self.conn.execute("INSERT INTO categories VALUES ('Dining', 'Discretionary')")
        _insert_tx(self.conn, 1, 'Chase', '2024-01-01', 'Starbucks', 'Dining', amount=-5.0)
        _insert_tx(self.conn, 2, 'Chase', '2024-02-01', 'Starbucks', 'Dining', amount=-6.0)
        _insert_tx(self.conn, 3, 'Chase', '2024-01-15', 'Other Vendor', 'Dining', amount=-20.0)

    def _categories(self):
        return {
            row[0]: row[1]
            for row in self.conn.execute(
                "SELECT Description, Category FROM consolidated_transactions"
            ).fetchall()
        }

    def test_all_matching_transactions_recategorized(self):
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', 'Groceries')
        cats = self._categories()
        self.assertEqual(cats['Starbucks'], 'Groceries')

    def test_non_matching_transactions_unchanged(self):
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', 'Groceries')
        cats = self._categories()
        self.assertEqual(cats['Other Vendor'], 'Dining')

    def test_new_vendor_mapping_inserted(self):
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', 'Groceries')
        result = self.conn.execute(
            "SELECT category FROM vendor_category_mapping WHERE vendor = 'Starbucks'"
        ).fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result[0], 'Groceries')

    def test_existing_vendor_mapping_updated(self):
        self.conn.execute(
            "INSERT INTO vendor_category_mapping (vendor, category) VALUES ('Starbucks', 'Dining')"
        )
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', 'Groceries')
        result = self.conn.execute(
            "SELECT category FROM vendor_category_mapping WHERE vendor = 'Starbucks'"
        ).fetchone()
        self.assertEqual(result[0], 'Groceries')

    def test_no_mapping_change_when_category_already_matches(self):
        self.conn.execute(
            "INSERT INTO vendor_category_mapping (vendor, category) VALUES ('Starbucks', 'Groceries')"
        )
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', 'Groceries')
        count = self.conn.execute(
            "SELECT COUNT(*) FROM vendor_category_mapping WHERE vendor = 'Starbucks'"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_mapping_deleted_when_excluded(self):
        self.conn.execute(
            "INSERT INTO vendor_category_mapping (vendor, category) VALUES ('Starbucks', 'Dining')"
        )
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', None)
        result = self.conn.execute(
            "SELECT category FROM vendor_category_mapping WHERE vendor = 'Starbucks'"
        ).fetchone()
        self.assertIsNone(result)

    def test_transactions_set_to_null_when_excluded(self):
        recategorize_all_vendor_transactions(self.conn, 'Starbucks', None)
        rows = self.conn.execute(
            "SELECT Category FROM consolidated_transactions WHERE Description = 'Starbucks'"
        ).fetchall()
        self.assertTrue(all(row[0] is None for row in rows))


if __name__ == '__main__':
    unittest.main()
