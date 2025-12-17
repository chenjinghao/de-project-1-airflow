import logging
import pendulum
import pandas_market_calendars


def is_holiday(
    proceed_task_id: str,
    end_task_id: str,
    timezone: str = "America/New_York",
    calendar: str = "NYSE",
) -> str:
    """
    Returns the task_id to follow based on whether 'today' is a market holiday.
    Args:
        timezone (str): Timezone to consider for 'today'. Default is "America/New_York".
        calendar (str): Market calendar to use. Default is "NYSE".
        proceed_task_id (str): Task ID to proceed with if not a holiday.
        end_task_id (str): Task ID to end the DAG if it is a holiday.
    """ 
    today = pendulum.today(timezone).to_date_string()

    cal = pandas_market_calendars.get_calendar(calendar)
    holidays = cal.holidays().holidays
    is_holiday = today in holidays

    if is_holiday:
        logging.info("%s is a holiday. Skipping stock data processing.", today)
        return end_task_id

    logging.info("%s is not a holiday. Proceeding with stock data processing.", today)
    return proceed_task_id

