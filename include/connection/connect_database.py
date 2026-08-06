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
from include.connection.gcs_adapter import GCSMinioAdapter
import os


def _connect_database():
    minio_conn = BaseHook.get_connection('minio')
    endpoint_url = minio_conn.extra_dejson.get('endpoint_url', '')

    # If targeting Google Cloud Storage, use the native GCS adapter
    if 'storage.googleapis.com' in endpoint_url:
        return GCSMinioAdapter()

    # Fallback to MinIO client for local/other S3 usage
    endpoint = endpoint_url.split('//')[1]
    secure = endpoint_url.startswith('https')

    access_key = minio_conn.login
    secret_key = minio_conn.password

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )
    return client

