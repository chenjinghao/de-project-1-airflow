import unittest
from unittest.mock import MagicMock, patch
import sys

# Mock modules that might be missing or complex
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.sdk'] = MagicMock()
sys.modules['airflow.sdk.bases'] = MagicMock()
sys.modules['airflow.sdk.bases.hook'] = MagicMock()
sys.modules['minio'] = MagicMock()

# Now we can import the module under test
from include.connection.connect_database import _connect_database

class TestConnectDatabase(unittest.TestCase):
    @patch('include.connection.connect_database.BaseHook')
    @patch('include.connection.connect_database.Minio')
    def test_connect_database_https(self, mock_minio, mock_base_hook):
        # Setup mock connection
        mock_conn = MagicMock()
        mock_conn.extra_dejson = {'endpoint_url': 'https://storage.googleapis.com'}
        mock_conn.login = 'access'
        mock_conn.password = 'secret'
        mock_base_hook.get_connection.return_value = mock_conn

        # Call the function
        _connect_database()

        # Verify Minio was called with secure=True
        mock_minio.assert_called_with(
            endpoint='storage.googleapis.com',
            access_key='access',
            secret_key='secret',
            secure=True
        )

    @patch('include.connection.connect_database.BaseHook')
    @patch('include.connection.connect_database.Minio')
    def test_connect_database_http(self, mock_minio, mock_base_hook):
        # Setup mock connection
        mock_conn = MagicMock()
        mock_conn.extra_dejson = {'endpoint_url': 'http://minio:9000'}
        mock_conn.login = 'access'
        mock_conn.password = 'secret'
        mock_base_hook.get_connection.return_value = mock_conn

        # Call the function
        _connect_database()

        # Verify Minio was called with secure=False
        mock_minio.assert_called_with(
            endpoint='minio:9000',
            access_key='access',
            secret_key='secret',
            secure=False
        )

if __name__ == '__main__':
    unittest.main()
