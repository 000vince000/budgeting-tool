import unittest
from unittest.mock import patch, MagicMock

import interaction


class TestMainMenuConnectionScoping(unittest.TestCase):
    """The menu should open a DB connection only while an action runs and close it
    immediately after, so the database lock is released while idling at the menu."""

    def test_db_action_opens_and_closes_one_connection(self):
        fake_conn = MagicMock()
        with patch.object(interaction, "get_user_choice", side_effect=[2, "x"]), \
             patch.object(interaction, "show_flagged_transactions") as handler, \
             patch.object(interaction.duckdb, "connect", return_value=fake_conn) as connect:
            result = interaction.main_menu("fake.db", 2026, 4)

        self.assertFalse(result)  # 'x' signals exit
        connect.assert_called_once_with("fake.db")
        handler.assert_called_once_with(fake_conn)
        fake_conn.close.assert_called_once()

    def test_connection_closed_even_if_action_raises(self):
        fake_conn = MagicMock()
        with patch.object(interaction, "get_user_choice", side_effect=[8, "x"]), \
             patch.object(interaction, "set_budget", side_effect=RuntimeError("boom")), \
             patch.object(interaction.duckdb, "connect", return_value=fake_conn):
            with self.assertRaises(RuntimeError):
                interaction.main_menu("fake.db", 2026, 4)

        fake_conn.close.assert_called_once()

    def test_non_db_choices_do_not_open_connection(self):
        # choice 1 (visualize, manages its own connection) then 'x' (exit)
        with patch.object(interaction, "get_user_choice", side_effect=[1, "x"]), \
             patch.object(interaction, "run_visualize_script") as visualize, \
             patch.object(interaction.duckdb, "connect") as connect:
            result = interaction.main_menu("fake.db", 2026, 4)

        self.assertFalse(result)
        visualize.assert_called_once_with(2026, 4)
        connect.assert_not_called()

    def test_dig_into_category_manages_own_connection(self):
        # choice 4 manages its own short-lived connections, so the shared menu
        # connection must not be opened for it.
        with patch.object(interaction, "get_user_choice", side_effect=[4, "x"]), \
             patch.object(interaction, "dig_into_category") as dig, \
             patch.object(interaction.duckdb, "connect") as connect:
            result = interaction.main_menu("fake.db", 2026, 4)

        self.assertFalse(result)
        dig.assert_called_once_with("fake.db", 2026, 4)
        connect.assert_not_called()

    def test_change_period_returns_true_without_connection(self):
        with patch.object(interaction, "get_user_choice", side_effect=[11]), \
             patch.object(interaction.duckdb, "connect") as connect:
            result = interaction.main_menu("fake.db", 2026, 4)

        self.assertTrue(result)  # 11 signals change-of-period
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
