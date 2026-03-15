"""Unit tests for the Bank API integration layer.

Covers:
  - token_store: save/load roundtrip, missing keys, file permissions
  - ingest_api._month_range: boundary conditions (Dec, Feb leap/non-leap)
  - ingest_api._apply_category_matching: auto-match paths (no interactive prompts)
  - ingest_api._apply_category_matching: EXCLUDE and manual paths (mocked get_category)
"""
import json
import os
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd


# ---------------------------------------------------------------------------
# token_store
# ---------------------------------------------------------------------------

class TestTokenStore(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".json", delete=False)
        self._tmp.close()
        # Point the module at the temp file for isolation
        import token_store
        self._orig = token_store.TOKEN_FILE
        token_store.TOKEN_FILE = Path(self._tmp.name)

    def tearDown(self):
        import token_store
        token_store.TOKEN_FILE = self._orig
        os.unlink(self._tmp.name)

    def test_save_and_load_roundtrip(self):
        import token_store
        token_store.save("plaid_chase", {"access_token": "tok_abc", "item_id": "item_1"})
        result = token_store.load("plaid_chase")
        self.assertEqual(result["access_token"], "tok_abc")
        self.assertEqual(result["item_id"], "item_1")

    def test_load_unknown_service_returns_none(self):
        import token_store
        token_store.save("schwab", {"token": "x"})
        self.assertIsNone(token_store.load("plaid_chase"))

    def test_load_missing_file_returns_none(self):
        import token_store
        token_store.TOKEN_FILE = Path("/tmp/__nonexistent_budgeting_token_test__.json")
        self.assertIsNone(token_store.load("anything"))

    def test_save_overwrites_same_service(self):
        import token_store
        token_store.save("svc", {"v": 1})
        token_store.save("svc", {"v": 2})
        self.assertEqual(token_store.load("svc")["v"], 2)

    def test_save_preserves_other_services(self):
        import token_store
        token_store.save("a", {"x": 1})
        token_store.save("b", {"y": 2})
        self.assertEqual(token_store.load("a")["x"], 1)
        self.assertEqual(token_store.load("b")["y"], 2)

    def test_file_permissions_owner_only(self):
        import token_store
        token_store.save("svc", {"k": "v"})
        mode = oct(Path(self._tmp.name).stat().st_mode)[-3:]
        self.assertEqual(mode, "600")


# ---------------------------------------------------------------------------
# ingest_api._month_range
# ---------------------------------------------------------------------------

class TestMonthRange(unittest.TestCase):
    def _range(self, s):
        from ingest_api import _month_range
        return _month_range(s)

    def test_standard_month(self):
        start, end = self._range("2025-03")
        self.assertEqual(start, date(2025, 3, 1))
        self.assertEqual(end, date(2025, 3, 31))

    def test_december_wraps_to_next_year(self):
        start, end = self._range("2024-12")
        self.assertEqual(start, date(2024, 12, 1))
        self.assertEqual(end, date(2024, 12, 31))

    def test_february_non_leap(self):
        _, end = self._range("2025-02")
        self.assertEqual(end, date(2025, 2, 28))

    def test_february_leap_year(self):
        _, end = self._range("2024-02")
        self.assertEqual(end, date(2024, 2, 29))

    def test_april_30_days(self):
        _, end = self._range("2025-04")
        self.assertEqual(end, date(2025, 4, 30))

    def test_invalid_format_raises(self):
        from ingest_api import _month_range
        with self.assertRaises((ValueError, IndexError)):
            _month_range("not-a-date")


# ---------------------------------------------------------------------------
# ingest_api._apply_category_matching
# ---------------------------------------------------------------------------

def _make_df(rows):
    return pd.DataFrame(
        rows,
        columns=["Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo"],
    )


def _run_matching(df, vendor_map, category_map, global_categories=None, user_choices=None):
    from ingest_api import _apply_category_matching
    return _apply_category_matching(
        df,
        vendor_map,
        category_map,
        global_categories or [],
        user_choices or {},
    )


class TestApplyCategoryMatchingAutoPath(unittest.TestCase):
    """Auto-match paths — no interactive prompts, no mocking needed."""

    def test_vendor_map_exact_match(self):
        df = _make_df([("Schwab", "01/01/2025", "AMAZON.COM", "", "ACH", -50.0, "")])
        result = _run_matching(df, vendor_map={"AMAZON.COM": "Shopping"}, category_map={})
        self.assertEqual(result.iloc[0]["Category"], "Shopping")
        self.assertIn("auto", result.iloc[0]["Memo"])

    def test_keyword_map_case_insensitive_match(self):
        df = _make_df([("Schwab", "01/02/2025", "WHOLE FOODS MARKET", "", "ACH", -80.0, "")])
        result = _run_matching(df, vendor_map={}, category_map={"whole foods": "Groceries"})
        self.assertEqual(result.iloc[0]["Category"], "Groceries")

    def test_vendor_map_takes_priority_over_keyword(self):
        df = _make_df([("Schwab", "01/03/2025", "STARBUCKS", "", "ACH", -6.0, "")])
        result = _run_matching(
            df,
            vendor_map={"STARBUCKS": "Coffee"},
            category_map={"star": "Entertainment"},
        )
        self.assertEqual(result.iloc[0]["Category"], "Coffee")

    def test_output_columns_are_standard(self):
        df = _make_df([("Chase", "02/15/2025", "NETFLIX", "", "digital", -15.0, "")])
        result = _run_matching(df, vendor_map={"NETFLIX": "Subscriptions"}, category_map={})
        self.assertListEqual(
            list(result.columns),
            ["Card", "Transaction Date", "Description", "Category", "Type", "Amount", "Memo"],
        )

    def test_multiple_rows_auto_matched(self):
        df = _make_df([
            ("Chase", "01/01/2025", "SPOTIFY", "", "digital", -10.0, ""),
            ("Chase", "01/02/2025", "NETFLIX", "", "digital", -15.0, ""),
        ])
        result = _run_matching(
            df,
            vendor_map={"SPOTIFY": "Subscriptions", "NETFLIX": "Subscriptions"},
            category_map={},
        )
        self.assertTrue((result["Category"] == "Subscriptions").all())

    def test_existing_memo_preserved_with_auto_suffix(self):
        df = _make_df([("Schwab", "01/04/2025", "AMAZON.COM", "", "ACH", -30.0, "existing")])
        result = _run_matching(df, vendor_map={"AMAZON.COM": "Shopping"}, category_map={})
        self.assertIn("existing", result.iloc[0]["Memo"])
        self.assertIn("auto", result.iloc[0]["Memo"])


class TestApplyCategoryMatchingInteractivePath(unittest.TestCase):
    """Interactive paths — get_category is mocked to avoid stdin dependency."""

    def test_exclude_sets_category_to_none(self):
        df = _make_df([("Schwab", "01/05/2025", "UNKNOWN VENDOR", "", "ACH", -9.0, "")])
        with patch("ingest.get_category", return_value=("EXCLUDE", True)):
            result = _run_matching(df, vendor_map={}, category_map={}, global_categories=["Food"])
        self.assertIsNone(result.iloc[0]["Category"])

    def test_user_chosen_category_applied(self):
        df = _make_df([("Schwab", "01/06/2025", "MYSTERY CAFE", "", "ACH", -12.0, "")])
        with patch("ingest.get_category", return_value=("Food", True)):
            result = _run_matching(df, vendor_map={}, category_map={}, global_categories=["Food"])
        self.assertEqual(result.iloc[0]["Category"], "Food")

    def test_user_intervened_appends_manual_to_memo(self):
        df = _make_df([("Schwab", "01/07/2025", "MYSTERY CAFE", "", "ACH", -12.0, "")])
        with patch("ingest.get_category", return_value=("Food", True)):
            result = _run_matching(df, vendor_map={}, category_map={}, global_categories=["Food"])
        self.assertIn("manual", result.iloc[0]["Memo"])

    def test_no_user_intervention_no_manual_memo(self):
        df = _make_df([("Schwab", "01/08/2025", "MYSTERY CAFE", "", "ACH", -12.0, "")])
        with patch("ingest.get_category", return_value=("Food", False)):
            result = _run_matching(df, vendor_map={}, category_map={}, global_categories=["Food"])
        self.assertNotIn("manual", result.iloc[0]["Memo"])


if __name__ == "__main__":
    unittest.main()
