from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.docker.operators.docker import DockerOperator
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

from include.stock_data_func.functions import _connect_database

# --- CONSTANTS ---
FUNCTION = 'TIME_SERIES_DAILY'
SYMBOL = 'JPM'
INTERVAL = '60min'
BUCKET_NAME = 'stock-data'


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock', 'price'],
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

def stock_price_dag():
    
    # --- Task Definitions ---

    @task
    def create_main_symbol_table():
        """Creates the main 'symbol' table if it doesn't exist to prevent errors on the first run."""
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        create_main_table_sql = """
        CREATE TABLE IF NOT EXISTS public.symbol (
            symbol VARCHAR PRIMARY KEY,
            latest_update_date TIMESTAMP(0)
        );
        """
        pg_hook.run(create_main_table_sql)
        logging.info("Ensured public.symbol table exists.")

    @task.branch
    def check_if_update_needed(symbol: str):
        """
        Checks if the stock data for the given symbol was already updated today.
        If yes, it follows the 'skip_update' path.
        If no, it follows the 'is_api_available' path.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        sql = f"SELECT latest_update_date FROM public.symbol WHERE symbol = '{symbol.upper()}'"
        result = pg_hook.get_first(sql)

        if result:
            last_update_date = result[0]
            if last_update_date.date() == datetime.today().date():
                logging.info(f"Symbol {symbol} already updated today. Skipping.")
                return 'skip_update'
        
        logging.info(f"Symbol {symbol} needs update. Proceeding with API fetch.")
        return 'is_api_available'

    @task.sensor(
        poke_interval=30,
        timeout=300,
        mode='poke'
    )
    def is_api_available() -> PokeReturnValue:
        import requests
        api = BaseHook.get_connection('stock_api')
        url = f'{api.host}'
        
        try:
            response = requests.get(
                url,
                params={'function': FUNCTION, 
                        'symbol': SYMBOL, 
                        # 'interval': INTERVAL, 
                        'apikey': api.password},
                timeout=10
            )
            response.raise_for_status()
            print(response.text)
            
            if response.status_code == 200 and 'Time Series' in response.text:
                return PokeReturnValue(is_done=True, xcom_value=response.json())
            else:
                return PokeReturnValue(is_done=False)
                
        except requests.exceptions.RequestException as e:
            logging.error(f"API not available: {e}")
            return PokeReturnValue(is_done=False)

    @task
    def extract_stock_data(api_response):
        """Process the stock data"""
        logging.info(f"Stock data received for processing.")
        return json.dumps(api_response)

    @task
    def store_stock_data(stock_data):
        client = _connect_database()
        bucket_name = BUCKET_NAME
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        stock = json.loads(stock_data)
        symbol = stock['Meta Data']['2. Symbol']
        data = json.dumps(stock['Time Series (Daily)'], ensure_ascii=False).encode('utf-8')

        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=f'{symbol}/prices.json',
            data=BytesIO(data),
            length=len(data)
        )
        logging.info(f"Stored raw data at {objw.bucket_name}/{symbol}/prices.json")
        return f"{objw.bucket_name}/{symbol}"

    # @task()
    # def get_formated_stock_data(path):
    #     client = _connect_database()
    #     prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    #     objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix_name, recursive=True)
    #     for obj in objects:
    #         if obj.object_name.endswith('.csv'):
    #             logging.info(f"Found formatted data at {obj.object_name}")
    #             return obj.object_name
            
    #     raise AirflowException("Formatted stock data not found.")

    @task
    def create_stock_price_table(csv_path: str):
        """Create stock-specific price table if not exist"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres')
        symbol = csv_path.split('/')[0].lower()
        create_symbol_table = f"""CREATE TABLE IF NOT EXISTS public."{symbol}" (
            timestamp TIMESTAMP PRIMARY KEY,
            open FLOAT, high FLOAT, low FLOAT, close FLOAT, volume BIGINT
        );"""
        postgres_hook.run(create_symbol_table)
        logging.info(f"Table created successfully for symbol: {symbol}")

    @task
    def load_to_database(csv_path: str):
        """Load CSV from MinIO to Postgres"""
        client = _connect_database()
        response = client.get_object(BUCKET_NAME, csv_path)
        df = pd.read_csv(response)
        response.close()
        response.release_conn()
        
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        table_name = csv_path.split('/')[0].lower()
        
        df.to_sql(
            name=table_name, con=engine, schema='public',
            if_exists='replace', index=False
        )
        logging.info(f"Loaded {len(df)} rows to {table_name} table")
        
    @task
    def update_latest_update_date(csv_path: str):
        """Update or insert latest_update_date in symbol table"""
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        symbol = csv_path.split('/')[0].upper()
        upsert_query = f"""
        INSERT INTO public.symbol (symbol, latest_update_date) VALUES ('{symbol}', NOW())
        ON CONFLICT (symbol) DO UPDATE SET latest_update_date = NOW();
        """
        pg_hook.run(upsert_query)
        logging.info(f"Updated/Inserted latest_update_date for symbol: {symbol}")

    # --- Task Instantiation and Dependency Flow ---

    # 1. Define all tasks and operators
    start_task = create_main_symbol_table()
    check_update_needed_task = check_if_update_needed(SYMBOL)
    
    # Branching paths
    skip_update_task = EmptyOperator(task_id='skip_update')
    api_available_task = is_api_available()

    # Main data pipeline tasks
    extracted_data = extract_stock_data(api_available_task)
    stored_data_path = store_stock_data(extracted_data)
    
    # format_data_task = DockerOperator(
    #     task_id='format_prices',
    #     image='airflow/stock-app',
    #     container_name='format_prices',
    #     api_version='auto',
    #     auto_remove='success',
    #     docker_url='tcp://host.docker.internal:2376',
    #     network_mode='container:spark-master',
    #     tty=True,
    #     xcom_all=False,
    #     mount_tmp_dir=False,
    #     environment={
    #         'SPARK_APPLICATION_ARGS': stored_data_path,
    #     },
    # )
    
    # formatted_data_path = get_formated_stock_data(stored_data_path)
    create_tables_task = create_stock_price_table(formatted_data_path)
    load_to_database_task = load_to_database(formatted_data_path)
    update_date_task = update_latest_update_date(formatted_data_path)

    # Task to merge the branches
    end_task = EmptyOperator(
        task_id='all_tasks_done',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # 2. Define the dependencies
    main_pipeline = [
        api_available_task,
        extracted_data,
        stored_data_path,
        format_data_task,
        formatted_data_path,
        create_tables_task,
        load_to_database_task,
        update_date_task
    ]

    start_task >> check_update_needed_task >> [api_available_task, skip_update_task]
    
    # Set dependencies for the main pipeline
    api_available_task >> extracted_data >> stored_data_path >> format_data_task >> formatted_data_path
    formatted_data_path >> create_tables_task >> load_to_database_task >> update_date_task

    # Merge the two branches into the final task
    update_date_task >> end_task
    skip_update_task >> end_task

stock_price_dag()