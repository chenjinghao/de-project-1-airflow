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

# tasks
from include.tasks.holiday_check import is_holiday
from include.tasks.create_today_folder import create_today_folder
from include.tasks.stock_info import extract_most_active_stocks, extract_price_top5_most_active_stocks, extract_news_top5_most_active_stocks,extract_insider_top5_most_active_stocks


@dag(
    start_date=datetime(2023, 1, 1),
    schedule="0 21 * * 1-5",
    catchup=False,
    tags=['stock', 'price','most_active'],
    max_active_runs=1,
    max_active_tasks=1,
    on_success_callback=SlackNotifier(
        slack_conn_id= 'slack',
        text=":tada: DAG {{ dag.dag_id }} Succeeded on {{ ds }}",
        channel='general'
    ),
    on_failure_callback=SlackNotifier(
        slack_conn_id= 'slack',
        text=":red_circle: DAG {{ dag.dag_id }} Failed on {{ ds }}",
        channel='general'
    )
)
def most_active_dag():

    end_task = EmptyOperator(task_id="end_task")

    # Task to check if today is a holiday
    @task.branch
    def holiday_check():
        holiday = is_holiday()
        if holiday:
            return end_task.task_id
        else:
            return "create_today_folder"
    
    @task(task_id="create_today_folder")
    def create_date_folder():
        return create_today_folder()
    
    # Use pool to limit concurrent API calls
    @task(task_id="extract_most_active_stocks", pool="api_pool")
    def most_active_stocks_task(folder_path, **context):
        return extract_most_active_stocks(folder_path, **context)
    
    @task(task_id="price_top5_most_active_stocks", pool="api_pool")
    def price_top5_most_active_stocks_task(folder_path,**context):
        return extract_price_top5_most_active_stocks(folder_path, **context)

    @task(task_id="news_top5_most_active_stocks", pool="api_pool")
    def news_top5_most_active_stocks_task(**context):
        return extract_news_top5_most_active_stocks(**context)
    
    @task(task_id="insider_top5_most_active_stocks", pool="api_pool")
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