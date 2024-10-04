import unittest
from unittest.mock import Mock, patch
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import transactions

class TestCalculateAndConditionallyInsertMonthlyBreakdowns(unittest.TestCase):

    @patch('transactions.date')
    @patch('transactions.db_operations')
    def test_calculate_and_conditionally_insert_monthly_breakdowns(self, mock_db_operations, mock_date):
        # Mock the current date
        mock_date.today.return_value = date(2023, 7, 1)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)

        # Mock database operations
        mock_conn = Mock()
        mock_db_operations.get_global_categories_from_db.return_value = ['Category1', 'Category2']
        mock_db_operations.get_latest_transaction_date.return_value = date(2023, 6, 30)
        mock_db_operations.get_active_breakdowns.return_value = {
            'breakdown': {'Category1': 0.6, 'Category2': 0.4}
        }
        mock_db_operations.get_net_income_for_month.return_value = 1000
        mock_db_operations.get_breakdown_items_by_date.return_value = Mock(empty=True)

        # Test data
        breakdown_id = 1
        effective_date = '2023-01-01'
        
        # Call the function
        transactions.calculate_and_conditionally_insert_monthly_breakdowns(
            mock_conn, breakdown_id, effective_date
        )

        # Assert that the function made the correct calls
        self.assertEqual(mock_db_operations.get_global_categories_from_db.call_count, 1)
        self.assertEqual(mock_db_operations.get_latest_transaction_date.call_count, 1)
        self.assertEqual(mock_db_operations.get_active_breakdowns.call_count, 1)
        
        # We expect 6 calls to get_net_income_for_month (Jan to Jun 2023)
        self.assertEqual(mock_db_operations.get_net_income_for_month.call_count, 6)
        
        # We expect 12 calls to get_breakdown_items_by_date and insert_surplus_deficit_breakdown_item
        # (2 categories per month for 6 months)
        self.assertEqual(mock_db_operations.get_breakdown_items_by_date.call_count, 12)
        self.assertEqual(mock_db_operations.insert_surplus_deficit_breakdown_item.call_count, 12)

        # Specific tests for 1000 * 0.6 = 600 and 1000 * 0.4 = 400
        calls = mock_db_operations.insert_surplus_deficit_breakdown_item.call_args_list
        for call in calls:
            _, _, category, _, amount, _ = call[0]
            if category == 'Category1':
                self.assertAlmostEqual(amount, 600, places=2,
                                       msg="Breakdown amount for Category1 should be 600")
            elif category == 'Category2':
                self.assertAlmostEqual(amount, 400, places=2,
                                       msg="Breakdown amount for Category2 should be 400")

        # Check some specific calls (keeping these for additional verification)
        mock_db_operations.insert_surplus_deficit_breakdown_item.assert_any_call(
            mock_conn, 1, 'Category1', 'Category1', 600, date(2023, 1, 1)
        )
        mock_db_operations.insert_surplus_deficit_breakdown_item.assert_any_call(
            mock_conn, 1, 'Category2', 'Category2', 400, date(2023, 1, 1)
        )
        mock_db_operations.insert_surplus_deficit_breakdown_item.assert_any_call(
            mock_conn, 1, 'Category1', 'Category1', 600, date(2023, 5, 1)
        )
        mock_db_operations.insert_surplus_deficit_breakdown_item.assert_any_call(
            mock_conn, 1, 'Category2', 'Category2', 400, date(2023, 5, 1)
        )

if __name__ == '__main__':
    unittest.main()