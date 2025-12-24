import json
from io import BytesIO
import requests as re
import logging
from airflow.sdk.bases.hook import BaseHook
from airflow.exceptions import AirflowException
from include.object_storage.connect_database import _connect_database
import time



def extract_most_active_stocks(folder_path, **context):

    """Extract most active stocks from Alpha Vantage API and store in MinIO"""
    logging.info("Extracting most active stocks data from API.")

    
    bucket_name = folder_path.split('/')[0]
    folder_name = folder_path.split('/')[1]
    
    api = BaseHook.get_connection('stock_api')
    url = f'{api.host}'
    
    try:
        response = re.get(
            url,
            params={'function': 'TOP_GAINERS_LOSERS', 
                    'apikey': api.password},
            timeout=10
        )
        response.raise_for_status()
        logging.info("Successfully retrieved most active stocks data.")
        
        most_active_stocks = response.json()
        most_active_stocks = most_active_stocks.get('most_actively_traded', [])
        context['ti'].xcom_push(key='most_active_stocks', value=most_active_stocks)
        
        
        client = _connect_database()
        data = json.dumps(most_active_stocks, ensure_ascii=False).encode('utf-8')

        objw = client.put_object(
            bucket_name=bucket_name,
            object_name=f'{folder_name}/most_active_stocks.json',
            data=BytesIO(data),
            length=len(data)
        )
        logging.info(f"Stored most active stocks data at {objw.bucket_name}/{folder_name}/most_active_stocks.json")
        return f"{objw.bucket_name}/{folder_name}/most_active_stocks.json"

    except re.exceptions.RequestException as e:
        logging.error(f"Failed to extract most active stocks data: {e}")
        raise AirflowException("Most active stocks API request failed.")
    
def extract_price_top5_most_active_stocks(file_path, **context):
    """Extract price data for top 5 most active stocks and store in MinIO"""
    logging.info("Extracting price data for top 5 most active stocks.")

    """Get the name of top 5 most active stocks name from pervious task"""
    logging.info("Retrieving top 5 most active stocks from pervious task.")

    api = BaseHook.get_connection('stock_api')
    url = f'{api.host}'

    try:
        client = _connect_database()
        bucket_name = file_path.split('/')[0]
        folder_name = file_path.split('/')[1]

        most_active_stocks = context['ti'].xcom_pull(key='most_active_stocks', task_ids='extract_most_active_stocks')
        top5_stocks = [stock['ticker'] for stock in most_active_stocks[:5]]
        context['ti'].xcom_push(key='top5_stocks', value=top5_stocks)

        for symbol in top5_stocks:
            stock_response = re.get(
                url,
                params={'function': 'TIME_SERIES_DAILY', 
                        'symbol': symbol,
                        'apikey': api.password},
                timeout=10
            )
            stock_response.raise_for_status()
            price_data = stock_response.json()

            data = json.dumps(price_data, ensure_ascii=False).encode('utf-8')

            objw = client.put_object(
                bucket_name=bucket_name,
                object_name=f'{folder_name}/price/{top5_stocks.index(symbol)}_{symbol}_stocks_price.json',
                data=BytesIO(data),
                length=len(data)
            )
            logging.info(f"Stored price data for {symbol} at {objw.bucket_name}/{folder_name}/price/{top5_stocks.index(symbol)}_{symbol}_stocks_price.json")
            time.sleep(2)  # To respect API rate limits
        return f"All price data for top 5 most active stocks stored in {bucket_name}/{folder_name}/price/"

    except re.exceptions.RequestException as e:
        logging.error(f"Failed to extract price data: {e}")
        raise AirflowException("Price data API request failed.")
    
def extract_news_top5_most_active_stocks(**context):
    """Placeholder for news extraction function for top 5 most active stocks"""
    logging.info("News extraction for top 5 most active stocks.")
    
    client = _connect_database()

    api = BaseHook.get_connection('stock_api')
    url = f'{api.host}'

    top5_stocks = context['ti'].xcom_pull(key='top5_stocks', task_ids='price_top5_most_active_stocks')
    folder_path = context['ti'].xcom_pull(key='return_value', task_ids='create_today_folder')
    bucket_name = folder_path.split('/')[0]
    folder_name = folder_path.split('/')[1]
    try:
        for symbol in top5_stocks:
            response = re.get(
                url,
                params={'function': 'NEWS_SENTIMENT', 
                        'tickers': symbol,
                        'apikey': api.password},
                timeout=10
            )
            response.raise_for_status()
            news_data = response.json()

            data = json.dumps(news_data, ensure_ascii=False).encode('utf-8')

            objw = client.put_object(
                bucket_name=bucket_name,
                object_name=f'{folder_name}/news/{top5_stocks.index(symbol)}_{symbol}_stocks_news.json',
                data=BytesIO(data),
                length=len(data)
            )

            logging.info(f"Stored news data for {symbol} at {objw.bucket_name}/{folder_name}/news/{top5_stocks.index(symbol)}_{symbol}_stocks_news.json")
            time.sleep(2)
        return f"All news data for top 5 most active stocks stored in {bucket_name}/{folder_name}/news/"
    
    except re.exceptions.RequestException as e:
        logging.error(f"Failed to extract news data: {e}")
        raise AirflowException("News data API request failed.")

def extract_insider_top5_most_active_stocks(**context):
    """Placeholder for news extraction function for top 5 most active stocks"""
    logging.info("News extraction for top 5 most active stocks.")
    
    client = _connect_database()

    api = BaseHook.get_connection('stock_api')
    url = f'{api.host}'

    top5_stocks = context['ti'].xcom_pull(key='top5_stocks', task_ids='price_top5_most_active_stocks')
    folder_path = context['ti'].xcom_pull(key='return_value', task_ids='create_today_folder')
    bucket_name = folder_path.split('/')[0]
    folder_name = folder_path.split('/')[1]
    try:
        for symbol in top5_stocks:
            response = re.get(
                url,
                params={'function': 'INSIDER_TRANSACTIONS', 
                        'symbol': symbol,
                        'apikey': api.password},
                timeout=10
            )
            response.raise_for_status()
            insider_data = response.json()

            data = json.dumps(insider_data, ensure_ascii=False).encode('utf-8')

            objw = client.put_object(
                bucket_name=bucket_name,
                object_name=f'{folder_name}/insider/{top5_stocks.index(symbol)}_{symbol}_stocks_insider.json',
                data=BytesIO(data),
                length=len(data)
            )

            logging.info(f"Stored insider data for {symbol} at {objw.bucket_name}/{folder_name}/insider/{top5_stocks.index(symbol)}_{symbol}_stocks_insider.json")
            time.sleep(2)  # To respect API rate limits
        return f"All insider data for top 5 most active stocks stored in {bucket_name}/{folder_name}/insider/"
    
    except re.exceptions.RequestException as e:
        logging.error(f"Failed to extract insider data: {e}")
        raise AirflowException("Insider data API request failed.")