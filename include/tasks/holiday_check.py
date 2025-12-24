import logging
import pendulum
import pandas_market_calendars


def is_holiday(
    timezone: str = "America/New_York",
    calendar: str = "NYSE",
) -> str:
    """
    Returns the task_id to follow based on whether 'today' is a market holiday.
    Args:
        timezone (str): Timezone to consider for 'today'. Default is "America/New_York".
        calendar (str): Market calendar to use. Default is "NYSE".
    Returns:
        bool: True if today is a holiday, False otherwise.
    """ 
    today = pendulum.today(timezone).to_date_string()

    cal = pandas_market_calendars.get_calendar(calendar)
    holidays = cal.holidays().holidays
    is_holiday = today in holidays

    if is_holiday:
        logging.info("%s is a holiday. Skipping stock data processing.", today)
        return True

    logging.info("%s is not a holiday. Proceeding with stock data processing.", today)
    return False

