import sys
import os
from unittest.mock import MagicMock

# Mocking modules to allow import of dags/most_active.py
modules_to_mock = [
    'airflow',
    'airflow.sdk',
    'airflow.sdk.bases',
    'airflow.sdk.bases.hook',
    'airflow.sdk.bases.sensor',
    'airflow.providers',
    'airflow.providers.postgres',
    'airflow.providers.postgres.hooks',
    'airflow.providers.postgres.hooks.postgres',
    'airflow.providers.standard',
    'airflow.providers.standard.operators',
    'airflow.providers.standard.operators.empty',
    'airflow.task',
    'airflow.task.trigger_rule',
    'airflow.exceptions',
    'airflow.providers.slack',
    'airflow.providers.slack.notifications',
    'airflow.providers.slack.notifications.slack',
    'airflow.providers.common',
    'airflow.providers.common.sql',
    'airflow.providers.common.sql.operators',
    'airflow.providers.common.sql.operators.sql',
    'pandas',
    'cosmos',
    'cosmos.profiles',
    'cosmos.profiles.postgres',
    'psycopg2',
    'psycopg2.extras',
    'psycopg2.sql',
    'minio',
    'pendulum',
    'pandas_market_calendars',
    'dlt',
    'numpy',
    'requests'
]

for mod in modules_to_mock:
    sys.modules[mod] = MagicMock()

# Set path
sys.path.append(os.getcwd())

def test_dag_import_syntax():
    # This import will fail if there are syntax errors or unmocked dependencies
    # It also executes the DAG definition code, verifying our os.getenv change doesn't crash
    try:
        import dags.most_active
    except ImportError as e:
        import traceback
        traceback.print_exc()
        raise e
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise e
