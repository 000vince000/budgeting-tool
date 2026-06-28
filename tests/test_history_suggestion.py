"""Tests for history-based category suggestions during ingest:
  - db_operations.get_category_history (real in-memory DuckDB)
  - ingest.get_category suggestion flow (Enter-accept, 'a' promote, split, amount source)
"""
import unittest
from unittest.mock import MagicMock, patch

import duckdb

import db_operations
import ingest


class TestGetCategoryHistoryQuery(unittest.TestCase):
    def setUp(self):
        self.conn = duckdb.connect(":memory:")
        self.conn.execute(
            'CREATE TABLE consolidated_transactions('
            'id BIGINT, Card VARCHAR, "Transaction Date" DATE, Description VARCHAR,'
            'Category VARCHAR, Type VARCHAR, Amount DECIMAL(18,2), Memo VARCHAR)')
        rows = [
            (1, "Schwab", "2026-01-01", "Check Paid #1", "Monthly fixed cost", "CHECK", -4650.00, ""),
            (2, "Schwab", "2026-02-01", "Check Paid #2", "Monthly fixed cost", "CHECK", -4650.00, ""),
            (3, "Chase", "2026-02-02", "WHOLEFOODS", "Groceries", "Sale", -50.00, ""),
            (4, "Chase", "2026-03-02", "WHOLEFOODS", "Groceries", "Sale", -30.00, ""),
            (5, "Chase", "2026-03-05", "WHOLEFOODS", "Discretionary", "Sale", -20.00, ""),
        ]
        for r in rows:
            self.conn.execute("INSERT INTO consolidated_transactions VALUES (?,?,?,?,?,?,?,?)", list(r))

    def tearDown(self):
        self.conn.close()

    def test_by_description_groups_and_orders(self):
        h = db_operations.get_category_history(self.conn, "WHOLEFOODS")
        self.assertEqual(h["by_description"], [("Groceries", 2), ("Discretionary", 1)])
        self.assertEqual(h["by_amount"], [])

    def test_by_amount_and_type_for_checks(self):
        h = db_operations.get_category_history(self.conn, "Check Paid #9", -4650.00, "CHECK")
        self.assertEqual(h["by_amount"], [("Monthly fixed cost", 2)])
        self.assertEqual(h["by_description"], [])  # varying check number -> no exact desc match

    def test_amount_skipped_without_type(self):
        h = db_operations.get_category_history(self.conn, "x", -4650.00, None)
        self.assertEqual(h["by_amount"], [])


class TestGetCategorySuggestionFlow(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cmap = {}
        self.cats = ["Groceries", "Discretionary", "Monthly fixed cost"]
        self.uc = {}

    @patch("ingest.get_category_history", return_value={"by_description": [("Groceries", 5)], "by_amount": []})
    @patch("builtins.input", return_value="")  # Enter
    def test_enter_accepts_unanimous_description(self, _inp, _hist):
        cat, intervened = ingest.get_category("WHOLEFOODS", self.cmap, self.cats, self.uc, self.conn, -50.0, "Sale")
        self.assertEqual(cat, "Groceries")
        self.assertTrue(intervened)
        self.assertNotIn("WHOLEFOODS", self.cmap)  # plain accept saves no rule

    @patch("ingest.insert_category_matching_pattern")
    @patch("ingest.get_category_history", return_value={"by_description": [("Groceries", 5)], "by_amount": []})
    @patch("builtins.input", return_value="a")  # always -> promote to rule
    def test_a_promotes_exact_vendor_to_rule(self, _inp, _hist, mock_ins):
        cat, _ = ingest.get_category("WHOLEFOODS", self.cmap, self.cats, self.uc, self.conn, -50.0, "Sale")
        self.assertEqual(cat, "Groceries")
        mock_ins.assert_called_once_with(self.conn, "WHOLEFOODS", "Groceries")
        self.assertEqual(self.cmap["WHOLEFOODS"], "Groceries")

    @patch("ingest.get_category_history",
           return_value={"by_description": [], "by_amount": [("Monthly fixed cost", 9)]})
    @patch("builtins.input", return_value="")  # Enter accepts amount/check suggestion
    def test_enter_accepts_amount_check(self, _inp, _hist):
        cat, _ = ingest.get_category("Check Paid #9", self.cmap, self.cats, self.uc, self.conn, -4650.0, "CHECK")
        self.assertEqual(cat, "Monthly fixed cost")
        self.assertNotIn("Check Paid #9", self.cmap)  # never a keyword rule for checks

    @patch("ingest._maybe_save_match_rule")
    @patch("ingest.get_category_history",
           return_value={"by_description": [], "by_amount": [("Monthly fixed cost", 9)]})
    @patch("builtins.input", side_effect=["a", "3"])  # 'a' invalid for amount source -> pick #3
    def test_a_ignored_for_amount_source(self, _inp, _hist, mock_save):
        cat, _ = ingest.get_category("Check Paid #9", self.cmap, self.cats, self.uc, self.conn, -4650.0, "CHECK")
        # sorted cats: Discretionary(1), Groceries(2), Monthly fixed cost(3), EXCLUDE(4)
        self.assertEqual(cat, "Monthly fixed cost")
        mock_save.assert_called_once()  # fell through to a normal numeric pick

    @patch("ingest._maybe_save_match_rule")
    @patch("ingest.get_category_history",
           return_value={"by_description": [("Groceries", 3), ("Discretionary", 2)], "by_amount": []})
    @patch("builtins.input", side_effect=["2"])  # split -> no Enter default; must pick
    def test_split_requires_explicit_pick(self, _inp, _hist, mock_save):
        cat, _ = ingest.get_category("AMAZON", self.cmap, self.cats, self.uc, self.conn, -20.0, "Sale")
        self.assertEqual(cat, "Groceries")  # #2 in sorted list
        mock_save.assert_called_once()


if __name__ == "__main__":
    unittest.main()
