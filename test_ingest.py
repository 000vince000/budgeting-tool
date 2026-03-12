import unittest
from unittest.mock import patch
import pandas as pd
import numpy as np

import ingest


class TestCurrencyToFloat(unittest.TestCase):
    def test_formatted_dollar_amount(self):
        self.assertAlmostEqual(ingest.currency_to_float("$1,234.56"), 1234.56)

    def test_zero_amount(self):
        self.assertAlmostEqual(ingest.currency_to_float("$0.00"), 0.0)

    def test_nan_input(self):
        self.assertEqual(ingest.currency_to_float(float('nan')), 0.0)

    def test_plain_float_passthrough(self):
        self.assertAlmostEqual(ingest.currency_to_float(42.5), 42.5)


class TestApplyCategoryMapping(unittest.TestCase):
    def setUp(self):
        self.category_map = {
            "amazon": "Shopping",
            "netflix": "Entertainment",
            "whole foods": "Groceries",
        }

    def test_substring_match(self):
        result = ingest.apply_category_mapping("Amazon Purchase", self.category_map)
        self.assertEqual(result, "Shopping")

    def test_no_match_returns_none(self):
        result = ingest.apply_category_mapping("Unknown Vendor XYZ", self.category_map)
        self.assertIsNone(result)

    def test_case_insensitive(self):
        result = ingest.apply_category_mapping("AMAZON PURCHASE", self.category_map)
        self.assertEqual(result, "Shopping")

    def test_first_match_wins(self):
        # "amazon" appears before "amazon prime" in insertion order
        category_map = {"amazon": "Shopping", "amazon prime": "Subscriptions"}
        result = ingest.apply_category_mapping("Amazon Prime Video", category_map)
        self.assertEqual(result, "Shopping")


class TestProcessChaseCsv(unittest.TestCase):
    def _make_df(self, rows):
        return pd.DataFrame(
            rows,
            columns=['Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo']
        )

    @patch('ingest.pd.read_csv')
    def test_automatic_payment_filtered(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'AUTOMATIC PAYMENT - THANK', 'Payment', 'Payment', -500.0, ''],
            ['2023-01-02', 'Starbucks', 'Food & Drink', 'Sale', -5.0, ''],
        ])

        result = ingest.process_chase_csv('Chase_1234.csv', [], {}, {})

        self.assertNotIn('AUTOMATIC PAYMENT - THANK', result['Description'].tolist())
        self.assertIn('Starbucks', result['Description'].tolist())

    @patch('ingest.pd.read_csv')
    def test_mapped_category_applied_without_user_input(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Amazon Purchase', 'Personal', 'Sale', -50.0, ''],
        ])
        category_map = {'amazon': 'Shopping'}

        with patch('ingest.get_category') as mock_get_category:
            result = ingest.process_chase_csv('Chase_1234.csv', ['Shopping'], {}, category_map)
            mock_get_category.assert_not_called()

        self.assertEqual(result.iloc[0]['Category'], 'Shopping')

    @patch('ingest.get_category')
    @patch('ingest.pd.read_csv')
    def test_exclude_sets_category_to_none(self, mock_read_csv, mock_get_category):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Some Weird Vendor', float('nan'), 'Sale', -10.0, ''],
        ])
        mock_get_category.return_value = ('EXCLUDE', True)

        result = ingest.process_chase_csv('Chase_1234.csv', ['Food & Drink'], {}, {})

        self.assertTrue(pd.isna(result.iloc[0]['Category']))

    @patch('ingest.pd.read_csv')
    def test_output_has_expected_columns(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Starbucks', 'Food & Drink', 'Sale', -5.0, ''],
        ])

        result = ingest.process_chase_csv('Chase_1234.csv', [], {}, {})

        self.assertEqual(
            set(result.columns),
            {'Card', 'Transaction Date', 'Description', 'Category', 'Type', 'Amount', 'Memo'}
        )


class TestProcessSchwabCsv(unittest.TestCase):
    def _make_df(self, rows):
        return pd.DataFrame(
            rows,
            columns=['Date', 'Description', 'Type', 'Withdrawal', 'Deposit']
        )

    @patch('ingest.pd.read_csv')
    def test_withdrawal_becomes_negative(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Some Expense', 'ACH', '$100.00', '$0.00'],
        ])

        result = ingest.process_schwab_csv(
            'schwab.csv', [], {}, {'some expense': 'Bills'}
        )

        self.assertAlmostEqual(result.iloc[0]['Amount'], -100.0)

    @patch('ingest.pd.read_csv')
    def test_deposit_becomes_positive(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Paycheck', 'ACH', '$0.00', '$2,000.00'],
        ])

        result = ingest.process_schwab_csv(
            'schwab.csv', [], {}, {'paycheck': 'Income'}
        )

        self.assertAlmostEqual(result.iloc[0]['Amount'], 2000.0)

    @patch('ingest.pd.read_csv')
    def test_chase_credit_rows_filtered(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'CHASE CREDIT CARD PAYMENT', 'ACH', '$500.00', '$0.00'],
            ['2023-01-02', 'Grocery Store', 'ACH', '$50.00', '$0.00'],
        ])

        result = ingest.process_schwab_csv(
            'schwab.csv', [], {}, {'grocery': 'Groceries'}
        )

        self.assertNotIn('CHASE CREDIT CARD PAYMENT', result['Description'].tolist())

    @patch('ingest.pd.read_csv')
    def test_transfer_rows_filtered(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Transfer to Savings', 'TRANSFER', '$200.00', '$0.00'],
            ['2023-01-02', 'Grocery Store', 'ACH', '$50.00', '$0.00'],
        ])

        result = ingest.process_schwab_csv(
            'schwab.csv', [], {}, {'grocery': 'Groceries'}
        )

        self.assertNotIn('Transfer to Savings', result['Description'].tolist())

    @patch('ingest.pd.read_csv')
    def test_category_mapping_applied(self, mock_read_csv):
        mock_read_csv.return_value = self._make_df([
            ['2023-01-01', 'Netflix Monthly', 'ACH', '$15.99', '$0.00'],
        ])

        result = ingest.process_schwab_csv(
            'schwab.csv', [], {}, {'netflix': 'Entertainment'}
        )

        self.assertEqual(result.iloc[0]['Category'], 'Entertainment')


if __name__ == '__main__':
    unittest.main()
