import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from include.object_storage.connect_database import _connect_database

def load_data_to_db():
    # step 1: connect to minio object storage and postgres database
    client = _connect_database()
    postgres_hook = PostgresHook(postgres_conn_id='postgres')

    # step 2: list objects in the specified bucket and prefix
    BUCKET_NAME = 'bronez'
    today_date = pd.Timestamp.now().strftime('%Y-%m-%d')
    prefix_name = f"{today_date}/most_active_stocks/"

    objects = client.list_objects(bucket_name=BUCKET_NAME, prefix=prefix_name, recursive=True)
    print(f"Objects found in {BUCKET_NAME} with prefix {prefix_name}:")
    for obj in objects:
        print(obj.object_name)
    
load_data_to_db()