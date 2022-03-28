import os
from datetime import timedelta

from google.cloud import storage
from constants import GCP_CREDS

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDS


def upload_file_2_bucket(**kwargs):
    ti = kwargs['ti']
    filename = ti.xcom_pull(task_ids='file_checker')
    csv_filename = filename[1]

    storage_client = storage.Client()
    bucket = storage_client.bucket('yellow_taxi_data_nyc')
    blob = bucket.blob(csv_filename)
    blob.upload_from_filename(f'partitions/{csv_filename}')


def get_signed_url(event):
    storage_client = storage.Client()
    bucket = storage_client.bucket(event['bucket'])
    blob = bucket.blob(event['name'])

    url = blob.generate_signed_url(
        version="v4",
        # This URL is valid for 15 minutes
        expiration=timedelta(minutes=15),
        # Allow GET requests using this URL.
        method="GET",
    )

    return url


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    url = get_signed_url(event)

    # df = pd.read_csv(url)

    # print(f"CSV data's shape is {df.shape}")
