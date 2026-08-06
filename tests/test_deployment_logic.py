import sys
import os
from unittest.mock import MagicMock

# Mock dependencies BEFORE importing the module under test
# We need to handle the specific imports used in include/connection/connect_database.py
# "from minio import Minio"
# "from airflow.sdk.bases.hook import BaseHook"

mock_minio_module = MagicMock()
sys.modules['minio'] = mock_minio_module

mock_airflow_sdk = MagicMock()
sys.modules['airflow.sdk'] = mock_airflow_sdk
sys.modules['airflow.sdk.bases'] = mock_airflow_sdk
mock_hook_module = MagicMock()
sys.modules['airflow.sdk.bases.hook'] = mock_hook_module

# Add root to path
sys.path.append(os.getcwd())

from include.connection.connect_database import _connect_database

def test_connect_database_https():
    # Setup mock connection
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {'endpoint_url': 'https://storage.googleapis.com'}
    mock_conn.login = 'user'
    mock_conn.password = 'pass'

    # Configure BaseHook.get_connection to return our mock
    mock_hook_module.BaseHook.get_connection.return_value = mock_conn

    # Execute
    _connect_database()

    # Verify
    mock_minio_module.Minio.assert_called_with(
        endpoint='storage.googleapis.com',
        access_key='user',
        secret_key='pass',
        secure=True
    )

def test_connect_database_http():
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {'endpoint_url': 'http://minio:9000'}
    mock_conn.login = 'user'
    mock_conn.password = 'pass'

    mock_hook_module.BaseHook.get_connection.return_value = mock_conn

    _connect_database()

    mock_minio_module.Minio.assert_called_with(
        endpoint='minio:9000',
        access_key='user',
        secret_key='pass',
        secure=False
    )

def test_connect_database_plain():
    mock_conn = MagicMock()
    mock_conn.extra_dejson = {'endpoint_url': 'minio:9000'}
    mock_conn.login = 'user'
    mock_conn.password = 'pass'

    mock_hook_module.BaseHook.get_connection.return_value = mock_conn

    _connect_database()

    mock_minio_module.Minio.assert_called_with(
        endpoint='minio:9000',
        access_key='user',
        secret_key='pass',
        secure=False
    )
