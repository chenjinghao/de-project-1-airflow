from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException
from airflow.providers.slack.notifications.slack import SlackNotifier
from datetime import datetime
import json
from io import BytesIO
import pandas as pd
import logging


from include.object_storage.connect_database import _connect_database

# tasks
from include.tasks.holiday_check import is_holiday
from include.tasks.create_today_folder import create_today_folder
from include.tasks.stock_info import extract_most_active_stocks, extract_price_top5_most_active_stocks, extract_news_top5_most_active_stocks,extract_insider_top5_most_active_stocks


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="0 21 * * 1-5", # Only run on weekdays at 9 PM EST (after market close)
    catchup=False,
    tags=['stock', 'price','most_active'],
    on_success_callback=SlackNotifier(
        slack_conn_id= 'slack_notification',
        text=":tada: DAG {{ dag.dag_id }} Succeeded on {{ ds }}",
        channel='general'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id= 'slack_notification',
        text=":red_circle: DAG {{ dag.dag_id }} Failed on {{ ds }}",
        channel='general'
    )
)
def most_active_dag():

    end_task = EmptyOperator(task_id="end_task")

    # Task to check if today is a holiday
    @task.branch
    def holiday_check():
        return is_holiday(
            proceed_task_id='create_today_folder',
            end_task_id=end_task.task_id,
            timezone="America/New_York",
            calendar="NYSE",
        )
    
    #Task to create folders in minio object storage name by date
    @task(task_id="create_today_folder")
    def create_date_folder():
        return create_today_folder()
    
    #Task to extract most active stocks and save to minio
    @task(task_id="extract_most_active_stocks")
    def most_active_stocks_task(folder_path, **context):
        return extract_most_active_stocks(folder_path, **context)
    
    #Task to extract price of top 5 most active stocks
    @task(task_id="price_top5_most_active_stocks")
    def price_top5_most_active_stocks_task(folder_path,**context):
        return extract_price_top5_most_active_stocks(folder_path, **context)

    #Task to extract news & sentiment of top 5 most active stocks
    @task(task_id="news_top5_most_active_stocks")
    def news_top5_most_active_stocks_task(**context):
        return extract_news_top5_most_active_stocks(**context)
    
    #Task to extract insider trading of top 5 most active stocks
    @task(task_id="insider_top5_most_active_stocks")
    def insider_top5_most_active_stocks_task(**context):
        return extract_insider_top5_most_active_stocks(**context)
    
    # Store task references
    holiday_check = holiday_check()
    create_folder = create_date_folder()
    most_active = most_active_stocks_task(create_folder)
    price_top5 = price_top5_most_active_stocks_task(most_active)
    news_top5 = news_top5_most_active_stocks_task()
    insider_top5 = insider_top5_most_active_stocks_task()

    # --- Task Dependencies ---
    holiday_check >> [create_folder, end_task]
    create_folder >> most_active >> price_top5 >> news_top5 >> insider_top5 >> end_task
most_active_dag()