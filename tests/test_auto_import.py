"""Unit tests for the auto-import-from-Downloads flow in ingest.py.

Covers:
  - detect_bank_from_header: Chase / Schwab / unrecognized
  - _archive_file: move + name-collision handling
  - auto_import_from_downloads: classification, archiving, skip-on-parse-error,
    and that unrecognized CSVs are left untouched (all with mocked DB writes).
"""
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import ingest

CHASE_HEADER = "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
SCHWAB_HEADER = '"Date","Status","Type","CheckNumber","Description","Withdrawal","Deposit","RunningBalance"\n'
JUNK_HEADER = "country,capital,population\n"


def _write(path, header, rows=""):
    with open(path, "w", newline="") as f:
        f.write(header + rows)


class TestDetectBank(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()

    def _make(self, name, header):
        p = os.path.join(self.dir, name)
        _write(p, header)
        return p

    def test_detects_chase(self):
        self.assertEqual(ingest.detect_bank_from_header(self._make("c.csv", CHASE_HEADER)), "chase")

    def test_detects_schwab(self):
        self.assertEqual(ingest.detect_bank_from_header(self._make("s.csv", SCHWAB_HEADER)), "schwab")

    def test_unrecognized_returns_none(self):
        self.assertIsNone(ingest.detect_bank_from_header(self._make("j.csv", JUNK_HEADER)))

    def test_missing_file_returns_none(self):
        self.assertIsNone(ingest.detect_bank_from_header(os.path.join(self.dir, "nope.csv")))


class TestArchiveFile(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.archive = os.path.join(self.dir, "imported")

    def test_moves_file(self):
        src = os.path.join(self.dir, "a.csv")
        _write(src, CHASE_HEADER)
        dest = ingest._archive_file(src, self.archive)
        self.assertFalse(os.path.exists(src))
        self.assertTrue(os.path.exists(dest))
        self.assertEqual(os.path.dirname(dest), self.archive)

    def test_name_collision_gets_suffix(self):
        # Pre-existing file with same name already archived.
        os.makedirs(self.archive)
        _write(os.path.join(self.archive, "a.csv"), CHASE_HEADER)
        src = os.path.join(self.dir, "a.csv")
        _write(src, CHASE_HEADER)
        dest = ingest._archive_file(src, self.archive)
        self.assertTrue(os.path.exists(os.path.join(self.archive, "a.csv")))
        self.assertEqual(os.path.basename(dest), "a_1.csv")


class TestAutoImport(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        _write(os.path.join(self.dir, "Chase5030_Activity.csv"), CHASE_HEADER)
        _write(os.path.join(self.dir, "Investor_Checking_Transactions.csv"), SCHWAB_HEADER)
        _write(os.path.join(self.dir, "austrian.csv"), JUNK_HEADER)
        self._orig_dir = ingest.DOWNLOADS_DIR
        ingest.DOWNLOADS_DIR = self.dir
        self.conn = MagicMock()

    def tearDown(self):
        ingest.DOWNLOADS_DIR = self._orig_dir

    def _row_df(self):
        return pd.DataFrame([{
            "Card": "X", "Transaction Date": "01/01/2026", "Description": "d",
            "Category": "C", "Type": "ACH", "Amount": -1.0, "Memo": "",
        }])

    @patch("builtins.input", return_value="")  # Enter = yes
    @patch("ingest.persist_data_in_db")
    def test_imports_and_archives_recognized_only(self, mock_persist, _mock_input):
        with patch("ingest.process_chase_csv", return_value=self._row_df()), \
             patch("ingest.process_schwab_csv", return_value=self._row_df()):
            ingest.auto_import_from_downloads(self.conn, [], {}, {}, {})

        # Persisted once, with both rows combined.
        mock_persist.assert_called_once()
        self.assertEqual(len(mock_persist.call_args[0][1]), 2)
        self.conn.commit.assert_called_once()

        archive = os.path.join(self.dir, "imported")
        # Recognized files moved out, junk + nothing else touched.
        self.assertFalse(os.path.exists(os.path.join(self.dir, "Chase5030_Activity.csv")))
        self.assertFalse(os.path.exists(os.path.join(self.dir, "Investor_Checking_Transactions.csv")))
        self.assertTrue(os.path.exists(os.path.join(self.dir, "austrian.csv")))
        self.assertEqual(sorted(os.listdir(archive)),
                         ["Chase5030_Activity.csv", "Investor_Checking_Transactions.csv"])

    @patch("builtins.input", return_value="")
    @patch("ingest.persist_data_in_db")
    def test_parse_error_leaves_file_in_place(self, mock_persist, _mock_input):
        # Chase parses, Schwab returns None (parse error) -> Schwab file not moved.
        with patch("ingest.process_chase_csv", return_value=self._row_df()), \
             patch("ingest.process_schwab_csv", return_value=None):
            ingest.auto_import_from_downloads(self.conn, [], {}, {}, {})

        self.assertEqual(len(mock_persist.call_args[0][1]), 1)
        self.assertTrue(os.path.exists(os.path.join(self.dir, "Investor_Checking_Transactions.csv")))
        self.assertFalse(os.path.exists(os.path.join(self.dir, "Chase5030_Activity.csv")))

    @patch("builtins.input", return_value="q")  # cancel
    @patch("ingest.persist_data_in_db")
    def test_cancel_does_nothing(self, mock_persist, _mock_input):
        with patch("ingest.process_chase_csv", return_value=self._row_df()), \
             patch("ingest.process_schwab_csv", return_value=self._row_df()):
            ingest.auto_import_from_downloads(self.conn, [], {}, {}, {})
        mock_persist.assert_not_called()
        self.assertTrue(os.path.exists(os.path.join(self.dir, "Chase5030_Activity.csv")))


if __name__ == "__main__":
    unittest.main()
