import unittest

from helpers import validate_date


class TestValidateDate(unittest.TestCase):
    def test_valid_date(self):
        self.assertTrue(validate_date("2023-01-15"))

    def test_leap_year_valid(self):
        self.assertTrue(validate_date("2000-02-29"))

    def test_wrong_order(self):
        self.assertFalse(validate_date("15-01-2023"))

    def test_wrong_separator(self):
        self.assertFalse(validate_date("01/15/2023"))

    def test_invalid_month(self):
        self.assertFalse(validate_date("2023-13-01"))

    def test_empty_string(self):
        self.assertFalse(validate_date(""))


if __name__ == '__main__':
    unittest.main()
