import os
import sys
from google.cloud import storage
from constants import GCP_CREDS

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDS

sys.path.append('../yellow_taxi_gcp')


def upload_file_2_bucket(bucket, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
