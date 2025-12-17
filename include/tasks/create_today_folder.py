from io import BytesIO
import logging
import pendulum
from include.object_storage.connect_database import _connect_database


def create_today_folder():
    client = _connect_database()
    bucket_name = 'bronze'
    today_date = pendulum.today("America/New_York").to_date_string()
    folder_name = f"{today_date}/"

    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logging.info(f'Created bucket {bucket_name}.')

    client.put_object(bucket_name, folder_name, BytesIO(b""), 0)
    
    logging.info(f'Created folder {folder_name} in bucket {bucket_name}.')
    
    return f"{bucket_name}/{folder_name}"