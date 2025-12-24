from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow.exceptions import AirflowException
from datetime import datetime
import json
from io import BytesIO
import pandas as pd
import logging

from include.object_storage.connect_database import _connect_database

FUNCTION = 'TIME_SERIES_INTRADAY'
SYMBOL = 'AAPL'
INTERVAL = '60min'
BUCKET_NAME='stock-data'


@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock', 'price']
)
def stock_price_dag_backup():
    
    @task.sensor(
        poke_interval=30,
        timeout=300,
        mode='poke'
    )
    def is_api_available() -> PokeReturnValue:
        import requests
        api = BaseHook.get_connection('stock_api')
        url = f'{api.host}{api.extra_dejson.get("endpoint")}'
        
        try:
            response = requests.get(
                url,
                params={'function': FUNCTION, 'symbol': SYMBOL, 'interval': INTERVAL, 'apikey': api.password},
                timeout=10
            )
            response.raise_for_status()
            
            # Check if API is available and responding correctly
            if response.status_code == 200 and 'Time Series' in response.text:
                return PokeReturnValue(is_done=True, xcom_value=response.json())
            else:
                return PokeReturnValue(is_done=False)
                
        except requests.exceptions.RequestException as e:
            print(f"API not available: {e}")
            return PokeReturnValue(is_done=False)

    @task
    def extract_stock_data(api_response):
        """Process the stock data"""
        print(f"Stock data received: {api_response}")
        # Add your data processing logic here
        return json.dumps(api_response)
    


    @task
    def store_stock_data(stock_data):
        client = _connect_database()

        bucket_name = BUCKET_NAME
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)


        stock = json.loads(stock_data)
        symbol = stock['Meta Data']['2. Symbol']
        data = json.dumps(stock['Time Series (60min)'], ensure_ascii=False).encode('utf-8')

        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=f'{symbol}/prices.json',
            data=BytesIO(data),
            length=len(data)
            )
        
        return f"{objw.bucket_name}/{symbol}"

    @task()
    def get_formated_stock_data(path):
        client = _connect_database()

        prefix_name = f"{path.split('/')[1]}/formatted_prices/"
        objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix_name, recursive=True)
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                return obj.object_name
            
        raise AirflowException("Formatted stock data not found.")
    

    @task
    def create_tables(csv_path: str):
        """Create tables if not exist"""
        postgres_hook = PostgresHook(postgres_conn_id='postgres')

        create_main_table = """CREATE TABLE IF NOT EXISTS public.symbol (
            symbol VARCHAR PRIMARY KEY,
            latest_update_date TIMESTAMP
        );"""

        # Extract symbol and use quoted identifier for safety
        symbol = csv_path.split('/')[0].lower()  # Convert to lowercase for consistency
        
        create_symbol_table = f"""CREATE TABLE IF NOT EXISTS public."{symbol}" (
            timestamp TIMESTAMP PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );"""

        postgres_hook.run(create_main_table)
        postgres_hook.run(create_symbol_table)

        logging.info(f"Tables created successfully: symbol and {symbol}")

    @task
    def load_to_database(csv_path: str):
        """Load CSV from MinIO to Postgres"""
        client = _connect_database()
        
        # Download CSV from MinIO
        response = client.get_object(BUCKET_NAME, csv_path)
        df = pd.read_csv(response)
        response.close()
        response.release_conn()
        
        # Load to Postgres
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Get table name from csv_path
        table_name = csv_path.split('/')[0].lower()
        
        df.to_sql(
            name=table_name,
            con=engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        
        print(f"Loaded {len(df)} rows to {table_name} table")
        
    @task
    def update_latest_update_date(csv_path: str):
        """Update or insert latest_update_date in symbol table"""
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        
        # Extract symbol from csv_path
        symbol = csv_path.split('/')[0].upper()
        
        # Use UPSERT (INSERT ... ON CONFLICT) to handle both insert and update
        upsert_query = f"""
        INSERT INTO public.symbol (symbol, latest_update_date)
        VALUES ('{symbol}', NOW())
        ON CONFLICT (symbol)
        DO UPDATE SET latest_update_date = NOW();
        """
        
        pg_hook.run(upsert_query)
        print(f"Updated/Inserted latest_update_date for symbol: {symbol}")

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
                print(f"Symbol {symbol} already updated today. Skipping.")
                return 'skip_update'
        
        print(f"Symbol {symbol} needs update. Proceeding with API fetch.")
        return 'is_api_available'

    skip_update = EmptyOperator(task_id='skip_update')

    # This task will run only if the main pipeline runs, not if it's skipped.
    # It needs a trigger rule to run after the branch.
    all_tasks_done = EmptyOperator(
        task_id='all_tasks_done',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    api_data = is_api_available()
    processed_data = extract_stock_data(api_data)
    store_data = store_stock_data(processed_data)
    format_stock_data = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove='success',
        docker_url='tcp://host.docker.internal:2376',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ ti.xcom_pull(task_ids="store_stock_data") }}',
        },
    )
    get_formated_stock_data_task = get_formated_stock_data(store_data)
    create_tables_task = create_tables(get_formated_stock_data_task)
    load_to_database_task = load_to_database(get_formated_stock_data_task)
    update_date_task = update_latest_update_date(get_formated_stock_data_task)

    # Set up the branching logic
    check_update_task = check_if_update_needed(SYMBOL)
    check_update_task >> [api_data, skip_update]
    
    # Define the main data processing pipeline
    api_data >> processed_data >> store_data >> format_stock_data >> get_formated_stock_data_task >> create_tables_task >> load_to_database_task >> update_date_task
    
    # Define what happens after the branches
    update_date_task >> all_tasks_done
    skip_update >> all_tasks_done

stock_price_dag_backup()