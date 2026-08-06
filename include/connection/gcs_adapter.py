from google.cloud import storage
import logging
import io

class MinioObject:
    def __init__(self, bucket_name, object_name):
        self.bucket_name = bucket_name
        self.object_name = object_name

class GCSResponse:
    def __init__(self, data):
        self.data = data

    def read(self):
        return self.data

    def close(self):
        pass

    def release_conn(self):
        pass

class GCSMinioAdapter:
    def __init__(self, project_id=None):
        self.client = storage.Client(project=project_id)

    def bucket_exists(self, bucket_name):
        try:
            bucket = self.client.bucket(bucket_name)
            return bucket.exists()
        except Exception as e:
            logging.error(f"Error checking if bucket exists: {e}")
            return False

    def make_bucket(self, bucket_name):
        try:
            bucket = self.client.bucket(bucket_name)
            if not bucket.exists():
                bucket.create(location="US") # Default to US or make configurable
                logging.info(f"Created bucket {bucket_name}")
        except Exception as e:
            logging.error(f"Error creating bucket: {e}")
            raise

    def list_objects(self, bucket_name, prefix=None, recursive=False):
        try:
            bucket = self.client.bucket(bucket_name)
            # GCS list_blobs is recursive by default if delimiter is not set
            delimiter = None if recursive else '/'
            blobs = self.client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

            objects = []
            for blob in blobs:
                objects.append(MinioObject(bucket_name, blob.name))

            # For non-recursive, simulate folders as objects if needed,
            # but list_blobs with delimiter puts them in prefixes
            if not recursive and blobs.prefixes:
                for p in blobs.prefixes:
                     objects.append(MinioObject(bucket_name, p))

            return objects
        except Exception as e:
            logging.error(f"Error listing objects: {e}")
            raise

    def put_object(self, bucket_name, object_name, data, length):
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)

            # data is BytesIO or similar
            if hasattr(data, 'read'):
                blob.upload_from_file(data, size=length)
            else:
                blob.upload_from_string(data)

            return MinioObject(bucket_name, object_name)
        except Exception as e:
            logging.error(f"Error putting object: {e}")
            raise

    def get_object(self, bucket_name, object_name):
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            content = blob.download_as_bytes()
            return GCSResponse(content)
        except Exception as e:
            logging.error(f"Error getting object: {e}")
            raise
