import sys
from unittest.mock import MagicMock, patch, ANY
import pytest

# Mock modules that might be missing in the test environment or need to be mocked
sys.modules['airflow'] = MagicMock()
sys.modules['airflow.sdk'] = MagicMock()
sys.modules['airflow.sdk.bases'] = MagicMock()
sys.modules['airflow.sdk.bases.hook'] = MagicMock()
sys.modules['airflow.exceptions'] = MagicMock()

# Mock providers
sys.modules['airflow.providers'] = MagicMock()
sys.modules['airflow.providers.postgres'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks'] = MagicMock()
sys.modules['airflow.providers.postgres.hooks.postgres'] = MagicMock()

sys.modules['airflow.providers.google'] = MagicMock()
sys.modules['airflow.providers.google.cloud'] = MagicMock()
sys.modules['airflow.providers.google.cloud.hooks'] = MagicMock()
sys.modules['airflow.providers.google.cloud.hooks.gcs'] = MagicMock()


# Mock Google modules structure
mock_google = MagicMock()
sys.modules['google'] = mock_google
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.storage'] = MagicMock()
sys.modules['google.oauth2'] = MagicMock()
sys.modules['google.oauth2.service_account'] = MagicMock()

# Mock psycopg2
mock_psycopg2 = MagicMock()
sys.modules['psycopg2'] = mock_psycopg2
sys.modules['psycopg2.extras'] = MagicMock()
sys.modules['psycopg2.sql'] = MagicMock()

# Import the module under test
# We rely on PYTHONPATH including the root directory
from include.tasks.load_2_db import load_2_db_biz_lookup

def test_load_2_db_biz_lookup():
    # Setup mocks
    mock_ti = MagicMock()
    mock_context = {'ds': '2023-10-27'}

    with patch('include.tasks.load_2_db._connect_database') as mock_connect_db, \
         patch('include.tasks.load_2_db.PostgresHook') as mock_postgres_hook, \
         patch('include.tasks.load_2_db.execute_values') as mock_execute_values, \
         patch('include.tasks.load_2_db.sql') as mock_sql:

        # Setup GCS client mock
        mock_client = MagicMock()
        mock_connect_db.return_value = mock_client

        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        # Setup list_blobs to return a list of blobs
        blob1 = MagicMock()
        blob1.name = "2023-10-27/business_info/1_AAPL_stocks_business_info.json"
        blob1.download_as_text.return_value = '{"Symbol": "AAPL", "Name": "Apple Inc."}'

        blob2 = MagicMock()
        blob2.name = "2023-10-27/business_info/2_GOOGL_stocks_business_info.json"
        blob2.download_as_text.return_value = '{"Symbol": "GOOGL", "Name": "Alphabet Inc."}'

        mock_client.list_blobs.return_value = [blob1, blob2]
        # When blob(name) is called, return the corresponding blob mock
        def blob_side_effect(name):
            if "AAPL" in name: return blob1
            if "GOOGL" in name: return blob2
            return MagicMock()

        mock_bucket.blob.side_effect = blob_side_effect

        # Setup Postgres mock
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_postgres_hook.return_value.get_conn.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # Mock connection details for logging
        mock_conn_details = MagicMock()
        mock_conn_details.host = "localhost"
        mock_conn_details.port = 5432
        mock_conn_details.schema = "public"
        mock_conn_details.login = "postgres"
        mock_postgres_hook.return_value.get_connection.return_value = mock_conn_details

        # Mock SQL composition
        mock_sql.Identifier.side_effect = lambda x: x
        mock_sql.SQL.side_effect = lambda x: x

        # Run the function
        load_2_db_biz_lookup(**mock_context)

        # Verify GCS calls
        assert mock_client.list_blobs.call_count == 1

        # Verify DB calls
        assert mock_execute_values.call_count == 1

        # Check that execute_values was called with generator/list
        args, _ = mock_execute_values.call_args
        cursor = args[0]
        query = args[1]
        data_generator = args[2]

        # Verify the data yielded by the generator
        data_list = list(data_generator)
        assert len(data_list) == 2

        # Checking content
        symbols = [row[0] for row in data_list] # Symbol is first column
        assert "AAPL" in symbols
        assert "GOOGL" in symbols
