"""Unit tests for the ingest 'save match rule' feature (teaching the substring
matcher during interactive categorization)."""
import unittest
from unittest.mock import MagicMock, patch

import ingest


class TestMaybeSaveMatchRule(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cmap = {}
        self.cats = ["Income", "Groceries"]

    @patch("ingest.insert_category_matching_pattern")
    @patch("ingest.insert_category")
    @patch("builtins.input", return_value="CAPITAL ONE SERV PAYROLL")
    def test_saves_trimmed_keyword(self, _inp, mock_ins_cat, mock_ins_pat):
        ingest._maybe_save_match_rule(
            self.conn, "CAPITAL ONE SERV PAYROLL 260529~ Tran: A", "Income",
            self.cmap, self.cats, is_new=False)
        mock_ins_pat.assert_called_once_with(self.conn, "CAPITAL ONE SERV PAYROLL", "Income")
        mock_ins_cat.assert_not_called()
        self.assertEqual(self.cmap["CAPITAL ONE SERV PAYROLL"], "Income")
        self.conn.commit.assert_called_once()

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input", return_value="")  # Enter = accept full description
    def test_enter_accepts_full_description(self, _inp, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "NETFLIX", "Income", self.cmap, self.cats, False)
        mock_ins_pat.assert_called_once_with(self.conn, "NETFLIX", "Income")

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input", return_value="-")  # skip
    def test_dash_skips_saving(self, _inp, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "NETFLIX", "Income", self.cmap, self.cats, False)
        mock_ins_pat.assert_not_called()
        self.assertEqual(self.cmap, {})

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input", return_value="ab")  # < 3 chars
    def test_too_short_keyword_skipped(self, _inp, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "NETFLIX", "Income", self.cmap, self.cats, False)
        mock_ins_pat.assert_not_called()

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input")
    def test_none_conn_skips_without_prompt(self, mock_input, mock_ins_pat):
        ingest._maybe_save_match_rule(None, "NETFLIX", "Income", self.cmap, self.cats, False)
        mock_input.assert_not_called()
        mock_ins_pat.assert_not_called()

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input")
    def test_exclude_skips_without_prompt(self, mock_input, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "SOME TRANSFER", "EXCLUDE", self.cmap, self.cats, False)
        mock_input.assert_not_called()
        mock_ins_pat.assert_not_called()

    @patch("ingest.insert_category_matching_pattern")
    @patch("ingest.insert_category")
    @patch("builtins.input", side_effect=["NETFLIX", "Discretionary"])  # keyword, then group
    def test_new_category_records_category_first(self, _inp, mock_ins_cat, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "NETFLIX.COM", "Streaming", self.cmap, self.cats, is_new=True)
        mock_ins_cat.assert_called_once_with(self.conn, "Streaming", "Discretionary")
        mock_ins_pat.assert_called_once_with(self.conn, "NETFLIX", "Streaming")
        self.assertIn("Streaming", self.cats)

    @patch("ingest.insert_category_matching_pattern")
    @patch("ingest.insert_category")
    @patch("builtins.input", side_effect=["NETFLIX", "nonsense-group"])
    def test_invalid_group_defaults_to_misc(self, _inp, mock_ins_cat, mock_ins_pat):
        ingest._maybe_save_match_rule(self.conn, "NETFLIX.COM", "Streaming", self.cmap, self.cats, is_new=True)
        mock_ins_cat.assert_called_once_with(self.conn, "Streaming", "Misc")


class TestGetCategoryWithConn(unittest.TestCase):
    @patch("ingest.insert_category_matching_pattern")
    @patch("ingest.insert_category")
    @patch("builtins.input", side_effect=["2", "WHOLEFOODS"])  # pick #2 (Income), then keyword
    def test_pick_then_save_rule(self, _inp, _mc, mock_ins_pat):
        conn = MagicMock()
        cmap, cats, uc = {}, ["Groceries", "Income"], {}
        result, intervened = ingest.get_category("WHOLEFOODS #123", cmap, cats, uc, conn)
        self.assertEqual(result, "Income")
        self.assertTrue(intervened)
        mock_ins_pat.assert_called_once_with(conn, "WHOLEFOODS", "Income")
        self.assertEqual(cmap["WHOLEFOODS"], "Income")

    @patch("ingest.insert_category_matching_pattern")
    @patch("builtins.input", return_value="1")  # no conn -> no save prompt consumed
    def test_no_conn_no_save_prompt(self, _inp, mock_ins_pat):
        cmap, cats, uc = {}, ["Groceries", "Income"], {}
        result, _ = ingest.get_category("FOO", cmap, cats, uc)  # conn defaults None
        self.assertEqual(result, "Groceries")
        mock_ins_pat.assert_not_called()


if __name__ == "__main__":
    unittest.main()
