import os

from google.cloud import storage
from .constants import GCP_CREDS

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDS


def upload_file_2_bucket(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='file_checker')
    csv_filename = filename[1]

    storage_client = storage.Client()
    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(csv_filename)
    blob.upload_from_filename(f'partitions/{csv_filename}')
