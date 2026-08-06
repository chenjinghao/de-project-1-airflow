# to connect to Google Cloud Storage, we can use the GCSHook from Airflow's Google Cloud provider package.
# from airflow.providers.google.cloud.hooks.gcs import GCSHook

# def _connect_database():
#     """
#     Connect to Google Cloud Storage using the 'google_cloud_default' Airflow connection.
#     Returns a google.cloud.storage.Client.
#     """
#     return GCSHook(gcp_conn_id='google_cloud_default').get_conn()

from minio import Minio
from airflow.sdk.bases.hook import BaseHook


def _connect_database():
    minio_conn = BaseHook.get_connection('minio')
    endpoint_url = minio_conn.extra_dejson.get('endpoint_url', '')

    secure = False
    if endpoint_url.startswith('https://'):
        secure = True
        endpoint = endpoint_url.replace('https://', '')
    elif endpoint_url.startswith('http://'):
        secure = False
        endpoint = endpoint_url.replace('http://', '')
    else:
        endpoint = endpoint_url

    access_key = minio_conn.login
    secret_key = minio_conn.password

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    return client

