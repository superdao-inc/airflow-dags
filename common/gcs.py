import logging
from datetime import datetime

from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from common.connections import GOOGLE_CLOUD_CONNECTION_ID

GCS_TEMP_BUCKET = "superdao-airflow-temp"
GCS_ETL_BUCKET = "superdao-etl-data"
GCS_CDN_BUCKET = "cdn-superdao"
MEGABYTE = 1024 * 1024

GCS_S3_COMPATIBLE_TEMP_BUCKET_ACCESS_KEY_ID = Variable.get(
    "gcs_temp_bucket_access_key_id__secret"
)
GCS_S3_COMPATIBLE_TEMP_BUCKET_SECRET = Variable.get("gcs_temp_bucket_secret")
GCS_S3_COMPATIBLE_ENDPOINT = "https://storage.googleapis.com"


def generate_gcs_object_name(dag_id, task_id, name, execution_dt=datetime.now()):
    return f'{dag_id}/{task_id}/{execution_dt.isoformat(timespec="seconds")}_{name}'


cloud_storage_hook = GCSHook(gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID)


# Helps avoid OverflowError: https://stackoverflow.com/questions/47610283/cant-upload-2gb-to-google-cloud-storage
# https://developers.google.com/api-client-library/python/guide/media_upload#resumable-media-chunked-upload
def upload_to_gcs(bucket, object, filename, content_type=None):
    logging.info("Uploading file to GCS: %s", object)

    service = cloud_storage_hook.get_conn()
    bucket = service.get_bucket(bucket)
    blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    blob.upload_from_filename(filename, content_type=content_type)


# Can download big files unlike gcs_hook.download which saves files in memory first
def download_from_gcs(bucket, object, filename):
    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blob_meta = bucket.get_blob(object)

    if blob_meta.size > 10 * MEGABYTE:
        blob = bucket.blob(object, chunk_size=10 * MEGABYTE)
    else:
        blob = bucket.blob(object)

    blob.download_to_filename(filename)


def get_objects_list(bucket, prefix):
    from google.cloud import storage

    storage_client = storage.Client()

    bucket = storage_client.get_bucket(bucket)
    blobs = bucket.list_blobs(prefix=prefix)

    return [blob.name for blob in blobs]
