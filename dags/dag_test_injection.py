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
import pendulum


# tasks
from include.tasks.holiday_check import is_holiday
from include.tasks.create_today_folder import create_today_folder
from include.tasks.stock_info import extract_most_active_stocks, extract_price_top5_most_active_stocks, extract_news_top5_most_active_stocks,extract_insider_top5_most_active_stocks

from include.object_storage.connect_database import _connect_database

@dag(
    start_date=datetime(2023, 1, 1),
    schedule="0 21 * * 1-5", # Only run on weekdays at 9 PM EST (after market close)
    catchup=False,
    tags=['stock', 'price','test'],
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
def test_dag():
    @task
    def load_2_db():

        # step 1: create if not exists database
        # Connect to default postgres database to check/create stocks_db
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        
        # Check if database exists
        check_db_query = """
        SELECT 1 FROM pg_database WHERE datname = 'stocks_db';
        """
        result = postgres_hook.get_first(check_db_query)
        
        if result:
            logging.info("Database 'stocks_db' already exists.")
        else:
            logging.info("Database 'stocks_db' does not exist. Creating...")
            conn = postgres_hook.get_conn()
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE stocks_db;")
            cursor.close()
            conn.close()
            logging.info("Database 'stocks_db' created successfully.")
        
        # step 2: Create necessary tables in stocks_db
        stocks_db_hook = PostgresHook(postgres_conn_id='stocks_db_conn')
        
        # Create tables in stocks_db
        create_table_most_active_stocks = """
        CREATE TABLE IF NOT EXISTS public.most_active_stocks (
            ticker TEXT PRIMARY KEY,
            change_amount NUMERIC(12,4),
            change_percentage NUMERIC(8,6),
            volume BIGINT,
            price jsonb,
            news jsonb,
            insider_trading jsonb,
            recorded_at TIMESTAMPTZ DEFAULT now()
            );
            """
        
        stocks_db_hook.run(create_table_most_active_stocks)
        logging.info("Table 'most_active_stocks' created in stocks_db database.")


        # step 3: get data from minio
        minio_client = _connect_database()
        today_date = pendulum.today("America/New_York").to_date_string()
        BUCKET_NAME = 'bronze'
        folder_name = f"{today_date}"

        ## Load json files from minio
        top5_most_active_stock = minio_client.get_object(
            bucket_name= BUCKET_NAME,
            object_name= f"{folder_name}/most_active_stocks.json"
        )

        ## get the list of top 5 most active stocks
        top5_stocks = json.loads(top5_most_active_stock.read().decode('utf-8'))[0:5]
        top5_stocks_df = pd.DataFrame(top5_stocks)
        logging.info(top5_stocks_df)


        logging.info(f"Top 5 most active stocks: {top5_stocks}")

        # ### load into stocks_db
        # most_active_stocks_data = json.loads(top5_most_active_stock.read().decode('utf-8'))
        # for stock in most_active_stocks_data:
        #     insert_query = """
        #     INSERT INTO public.most_active_stocks (ticker, change_amount, change_percentage, volume, price, news, insider_trading)
        #     VALUES (%s, %s, %s, %s, %s, %s, %s)
        #     ON CONFLICT (ticker) DO UPDATE 
        #     SET change_amount = EXCLUDED.change_amount,
        #         change_percentage = EXCLUDED.change_percentage,
        #         volume = EXCLUDED.volume,
        #         price = EXCLUDED.price,
        #         news = EXCLUDED.news,
        #         insider_trading = EXCLUDED.insider_trading,
        #         recorded_at = now();
        #     """
        #     stocks_db_hook.run(
        #         insert_query,
        #         parameters=(
        #             stock['ticker'],
        #             stock['change_amount'],
        #             stock['change_percentage'],
        #             stock['volume'],
        #             json.dumps(stock['price']),
        #             json.dumps(stock['news']),
        #             json.dumps(stock['insider_trading'])
        #         )
        #     )


        

    
    load_2_db_task = load_2_db()
    
    load_2_db_task

test_dag()
