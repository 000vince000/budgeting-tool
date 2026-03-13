import io
import unittest
from datetime import date
from unittest.mock import patch

from dateutil.relativedelta import relativedelta

from helpers import (
    get_user_choice,
    get_user_input,
    get_user_specified_date,
    print_divider,
    print_numbered_list,
    validate_date,
)


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


class TestPrintNumberedList(unittest.TestCase):
    def test_default_start(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_numbered_list(["apple", "banana", "cherry"])
            output = mock_out.getvalue()
        self.assertIn("1. apple", output)
        self.assertIn("2. banana", output)
        self.assertIn("3. cherry", output)

    def test_custom_start(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_numbered_list(["x", "y"], start=5)
            output = mock_out.getvalue()
        self.assertIn("5. x", output)
        self.assertIn("6. y", output)

    def test_empty_list(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_numbered_list([])
            output = mock_out.getvalue()
        self.assertEqual(output, "")


class TestGetUserChoice(unittest.TestCase):
    def test_valid_numeric_choice(self):
        with patch("builtins.input", side_effect=["2"]):
            result = get_user_choice("Pick: ", range(1, 4))
        self.assertEqual(result, 2)

    def test_x_in_valid_range(self):
        with patch("builtins.input", side_effect=["x"]):
            result = get_user_choice("Pick: ", ["x", 1, 2])
        self.assertEqual(result, "x")

    def test_out_of_range_then_valid(self):
        with patch("builtins.input", side_effect=["9", "1"]):
            result = get_user_choice("Pick: ", range(1, 4))
        self.assertEqual(result, 1)

    def test_non_numeric_then_valid(self):
        with patch("builtins.input", side_effect=["abc", "3"]):
            result = get_user_choice("Pick: ", range(1, 4))
        self.assertEqual(result, 3)


class TestGetUserInput(unittest.TestCase):
    def test_valid_string_input(self):
        with patch("builtins.input", side_effect=["hello"]):
            result = get_user_input("Enter: ")
        self.assertEqual(result, "hello")

    def test_valid_int_input(self):
        with patch("builtins.input", side_effect=["42"]):
            result = get_user_input("Enter: ", input_type=int)
        self.assertEqual(result, 42)

    def test_invalid_type_then_valid(self):
        with patch("builtins.input", side_effect=["abc", "7"]):
            result = get_user_input("Enter: ", input_type=int)
        self.assertEqual(result, 7)

    def test_validation_func_rejects_then_passes(self):
        with patch("builtins.input", side_effect=["bad", "good"]):
            result = get_user_input("Enter: ", validation_func=lambda x: x == "good")
        self.assertEqual(result, "good")


class TestPrintDivider(unittest.TestCase):
    def test_contains_title(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_divider("My Section")
            output = mock_out.getvalue()
        self.assertIn("My Section", output)

    def test_contains_separator(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_divider("Title")
            output = mock_out.getvalue()
        self.assertIn("=" * 50, output)

    def test_title_centered(self):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            print_divider("Hi")
            output = mock_out.getvalue()
        self.assertIn("Hi".center(50), output)


class TestGetUserSpecifiedDate(unittest.TestCase):
    def test_blank_inputs_use_defaults(self):
        today = date.today()
        last_month = today - relativedelta(months=1)
        with patch("builtins.input", side_effect=["", ""]):
            year, month = get_user_specified_date()
        self.assertEqual(year, today.year)
        self.assertEqual(month, last_month.month)

    def test_explicit_year_and_month(self):
        with patch("builtins.input", side_effect=["2024", "6"]):
            year, month = get_user_specified_date()
        self.assertEqual(year, 2024)
        self.assertEqual(month, 6)

    def test_invalid_year_string_then_valid(self):
        with patch("builtins.input", side_effect=["notayear", "2023", "3"]):
            year, month = get_user_specified_date()
        self.assertEqual(year, 2023)
        self.assertEqual(month, 3)

    def test_out_of_range_year_then_valid(self):
        with patch("builtins.input", side_effect=["999", "2023", "3"]):
            year, month = get_user_specified_date()
        self.assertEqual(year, 2023)
        self.assertEqual(month, 3)

    def test_invalid_month_string_then_valid(self):
        # Invalid month causes outer loop to restart, so year is prompted again
        with patch("builtins.input", side_effect=["2023", "notamonth", "2023", "5"]):
            year, month = get_user_specified_date()
        self.assertEqual(year, 2023)
        self.assertEqual(month, 5)

    def test_out_of_range_month_then_valid(self):
        # Out-of-range month causes outer loop to restart, so year is prompted again
        with patch("builtins.input", side_effect=["2023", "13", "2023", "1"]):
            year, month = get_user_specified_date()
        self.assertEqual(year, 2023)
        self.assertEqual(month, 1)


if __name__ == "__main__":
    unittest.main()
