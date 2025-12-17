from minio import Minio
from airflow.sdk.bases.hook import BaseHook

def _connect_database():
    minio_conn = BaseHook.get_connection('minio')
    endpoint = minio_conn.extra_dejson['endpoint_url'].split('//')[1]
    # endpoint = minio_conn.host
    access_key = minio_conn.login
    secret_key = minio_conn.password

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )
    return client