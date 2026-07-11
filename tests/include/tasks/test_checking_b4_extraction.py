import unittest
from unittest.mock import patch, MagicMock
import numpy as np
from include.tasks.checking_b4_extraction import is_holiday

class TestIsHoliday(unittest.TestCase):

    @patch('include.tasks.checking_b4_extraction.pandas_market_calendars')
    @patch('include.tasks.checking_b4_extraction.pendulum')
    def test_is_holiday_true(self, mock_pendulum, mock_pmc):
        # Mock pendulum to return a specific date
        mock_today = MagicMock()
        mock_today.to_date_string.return_value = '2023-01-01'
        mock_pendulum.today.return_value = mock_today

        # Mock calendar
        mock_cal = MagicMock()
        mock_pmc.get_calendar.return_value = mock_cal

        # Scenario 1: holidays() returns object with .holidays attribute (tuple of np.datetime64)
        mock_holidays_obj = MagicMock()
        # Create a tuple containing the target date as np.datetime64
        holiday_date = np.datetime64('2023-01-01')
        mock_holidays_obj.holidays = (holiday_date,)
        mock_cal.holidays.return_value = mock_holidays_obj

        self.assertTrue(is_holiday())

    @patch('include.tasks.checking_b4_extraction.pandas_market_calendars')
    @patch('include.tasks.checking_b4_extraction.pendulum')
    def test_is_holiday_false(self, mock_pendulum, mock_pmc):
        mock_today = MagicMock()
        mock_today.to_date_string.return_value = '2023-01-02'
        mock_pendulum.today.return_value = mock_today

        mock_cal = MagicMock()
        mock_pmc.get_calendar.return_value = mock_cal

        mock_holidays_obj = MagicMock()
        mock_holidays_obj.holidays = (np.datetime64('2023-01-01'),)
        mock_cal.holidays.return_value = mock_holidays_obj

        self.assertFalse(is_holiday())

    @patch('include.tasks.checking_b4_extraction.pandas_market_calendars')
    @patch('include.tasks.checking_b4_extraction.pendulum')
    def test_is_holiday_direct_list(self, mock_pendulum, mock_pmc):
        # Scenario 2: holidays() returns list/tuple directly (no .holidays attribute)
        # This mocks the behavior that might cause issues if not handled.

        mock_today = MagicMock()
        mock_today.to_date_string.return_value = '2023-01-01'
        mock_pendulum.today.return_value = mock_today

        mock_cal = MagicMock()
        mock_pmc.get_calendar.return_value = mock_cal

        # Directly return tuple, mocking the case where holidays() returns the list directly
        # If the code assumes .holidays attribute exists, this mock setup will cause failure
        # ONLY IF the code accesses .holidays on this tuple.
        # But wait, MagicMock usually allows attribute access returning another mock unless spec is set.
        # However, if I return a real tuple, it will fail.
        mock_cal.holidays.return_value = (np.datetime64('2023-01-01'),)

        # This test verifies that the function can handle this return type gracefully.
        # The assertion failure message will help confirm if the fix works.
        try:
            result = is_holiday()
            self.assertTrue(result)
        except AttributeError:
            # We expect this to fail BEFORE the fix
            # But the test should pass AFTER the fix
            # To make the test fail now (so I can see it fail), I re-raise or fail.
            raise
